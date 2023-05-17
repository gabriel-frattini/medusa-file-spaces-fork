import { AbstractFileService } from "@medusajs/medusa";
import aws from "aws-sdk";
import fs from "fs";
import { parse } from "path";
import stream from "stream";

class DigitalOceanService extends AbstractFileService {
  constructor({}, options) {
    super({}, options);

    this.bucket_ = options.bucket;
    this.spacesUrl_ = options.spaces_url?.replace(/\/$/, "");
    this.accessKeyId_ = options.access_key_id;
    this.secretAccessKey_ = options.secret_access_key;
    this.region_ = options.region;
    this.endpoint_ = options.endpoint;
    this.downloadUrlDuration = options.download_url_duration ?? 60; // 60 seconds
    this.directory_ = options.directory;

    console.log("Options -> ", options);
  }

  upload(file) {
    this.updateAwsConfig();

    return this.uploadFile(file);
  }

  uploadProtected(file) {
    this.updateAwsConfig();

    return this.uploadFile(file, { acl: "public-read" });
  }

  uploadFile(file, options = { isProtected: false, acl: undefined }) {
    const parsedFilename = parse(file.originalname);
    const fileKey = this.directory_
      ? `${this.directory_}/${parsedFilename.name}-${Date.now()}${
          parsedFilename.ext
        }`
      : `${parsedFilename.name}-${Date.now()}${parsedFilename.ext}`;

    console.log("uploadFile fileKey -> ", fileKey);
    const s3 = new aws.S3();
    const params = {
      ACL: options.acl ?? (options.isProtected ? "private" : "public-read"),
      Bucket: this.bucket_,
      Body: fs.createReadStream(file.path),
      Key: fileKey,
    };

    return new Promise((resolve, reject) => {
      s3.upload(params, (err, data) => {
        if (err) {
          console.log("Error uploading file: ", err);
          reject(err);
          return;
        }

        if (this.spacesUrl_) {
          resolve({ url: `${this.spacesUrl_}/${data.Key}`, key: data.Key });
        }
        console.log("Uploaded file: ", data);
        resolve({ url: data.Location, key: data.Key });
      });
    });
  }

  async delete(file) {
    this.updateAwsConfig();

    const s3 = new aws.S3();
    const params = {
      Bucket: this.bucket_,
      Key: `${file}`,
    };

    return new Promise((resolve, reject) => {
      s3.deleteObject(params, (err, data) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(data);
      });
    });
  }

  async getUploadStreamDescriptor(fileData) {
    this.updateAwsConfig();

    const pass = new stream.PassThrough();

    const fileKey = `${fileData.name}.${fileData.ext}`;
    console.log("getUploadStreamDescriptor fileKey -> ", fileKey);
    const params = {
      ACL: fileData.acl ?? "private",
      Bucket: this.bucket_,
      Body: pass,
      Key: fileKey,
    };

    const s3 = new aws.S3();
    return {
      writeStream: pass,
      promise: s3.upload(params).promise(),
      url: `${this.spacesUrl_}/${fileKey}`,
      fileKey,
    };
  }

  async getDownloadStream(fileData) {
    this.updateAwsConfig();

    const s3 = new aws.S3();

    console.log("getDownloadStream fileKey -> ", fileData.fileKey);
    const params = {
      Bucket: this.bucket_,
      Key: `${fileData.fileKey}`,
    };

    return s3.getObject(params).createReadStream();
  }

  async getPresignedDownloadUrl(fileData) {
    this.updateAwsConfig({
      signatureVersion: "v4",
    });

    const s3 = new aws.S3();

    console.log("getPresignedDownloadUrl fileKey -> ", fileData.fileKey);
    const params = {
      Bucket: this.bucket_,
      Key: `${fileData.fileKey}`,
      Expires: this.downloadUrlDuration,
    };

    return await s3.getSignedUrlPromise("getObject", params);
  }

  updateAwsConfig(additionalConfiguration = {}) {
    aws.config.setPromisesDependency(null);
    aws.config.update(
      {
        accessKeyId: this.accessKeyId_,
        secretAccessKey: this.secretAccessKey_,
        region: this.region_,
        endpoint: this.endpoint_,
        ...additionalConfiguration,
      },
      true
    );
    console.log("AWS Config -> ", aws.config);
  }
}

export default DigitalOceanService;
