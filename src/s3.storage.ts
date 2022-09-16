import { CreateMultipartUploadCommandOutput, S3 } from '@aws-sdk/client-s3';
import { ResponseMetadata } from '@aws-sdk/client-s3/dist-es/commands'

type UploadInfo<T> = {
  id: string;
  filename: string;
  size: number | string;
  chunks: number;
  processing: boolean;
  metadata: ResponseMetadata;
  data: T;
  parts: Array<{ PartNumber: number; ETag: string }>;
};


class S3WithPartsStorage<T = {clientHash: string}> {
  private s3: S3 = null;

  constructor({ region, secret, key }) {
    this.s3 = new S3({
      region: process.env.AWS_ACCESS_REGION || region,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || key,
        secretAccessKey: process.env.AWS_ACCESS_KEY_SECRET || secret,
      },
    });
  }

  public async _handleFile(
    _req: Express.Request,
    file: Express.Multer.File,
    callback: (err: Error, data: any) => void,
  ) {
    
    if (typeof file.fieldname === "string") {
      try {
        file.fieldname = JSON.parse(file.fieldname)
      } catch (err) {
        file.fieldname = {} as string
      }
    }

    const uploadInfo: UploadInfo<T> = {
      id: null,
      filename: null,
      size: file.size || 0,
      chunks: 0,
      metadata: null,
      processing: false,
      data: file.fieldname as unknown as T,
      parts: [],
    };

    const promisePartsList: Array<Promise<{ 
      PartNumber: number; 
      ETag: string 
    }>> = [];

    //@ts-ignore
    uploadInfo.filename = `${uploadInfo.data.clientHash ? `${uploadInfo.data.clientHash}/` : ''}${file.originalname}`

    const cmuco = await this.createHeatmapToStorage(uploadInfo);
    uploadInfo.id = cmuco.UploadId;
    uploadInfo.metadata = cmuco.$metadata

    console.info('Starting upload of %s', file.originalname);

    file.stream.on('readable', () => {
      if (uploadInfo.processing) {
        return;
      }

      return file.stream.read(9e6);
    });

    file.stream.on('data', (chunk: Buffer) => {
      uploadInfo.processing = true;

      const actualChunk = uploadInfo.chunks + 1;
      uploadInfo.chunks = actualChunk;

      console.info(
        'Uploading part %s of [ %s ]',
        actualChunk,
        file.originalname,
      );

      uploadInfo.size = file.size || +uploadInfo.size + chunk.length;

      promisePartsList.push(
        new Promise(async (resolve) => {
          const uploadedResult = await this.uploadHeatmapToStorage(
            chunk,
            uploadInfo,
            actualChunk,
          );

          console.info(
            'Uploading completed of part %s of [ %s ]',
            actualChunk,
            file.originalname,
          );

          return resolve({
            PartNumber: actualChunk,
            ETag: uploadedResult.ETag,
          });
        }),
      );

      uploadInfo.processing = false;

      file.stream.emit('readable');
    });

    file.stream.on('end', async () => {
      uploadInfo.parts = await Promise.all(promisePartsList);

      await this.completeStorageUpload(uploadInfo);

      console.info('Upload of [ %s ] is completed!', file.originalname);

      delete uploadInfo.processing;

      return callback(null, uploadInfo);
    });
  }

  async createHeatmapToStorage(info: UploadInfo<T>): Promise<CreateMultipartUploadCommandOutput> {
    return this.s3.createMultipartUpload({
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: `${info.filename}`,
    });
  }

  async uploadHeatmapToStorage(
    file: string | Buffer,
    info: UploadInfo<T>,
    partNumber,
  ) {
    
    return this.s3.uploadPart({
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: `${info.filename}`,
      PartNumber: partNumber,
      UploadId: info.id,
      Body: file,
    });
  }

  async completeStorageUpload(info:  UploadInfo<T>) {
    return this.s3.completeMultipartUpload({
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: `${info.filename}`,
      UploadId: info.id,
      MultipartUpload: {
        Parts: info.parts,
      },
    });
  }

  async abortStorageUpload(info:  UploadInfo<T>) {
    return this.s3.abortMultipartUpload({
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: `${info.filename}`,
      UploadId: info.id,
    });
  }
}

export function S3CustomStorage<T>(opts) {
  return new S3WithPartsStorage<T>(opts);
}