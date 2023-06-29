import { defineDriver } from 'unstorage';
import { S3Client, GetObjectCommand, HeadObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';

const DRIVER_NAME = 's3';

export interface S3Options {
  bucket: string;
  prefix?: string;
  region?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
}

let s3Client: S3Client;

export const s3StorageDriver = defineDriver<S3Options>((options) => {
  const { bucket, prefix, region, accessKeyId, secretAccessKey } = options;
  if (secretAccessKey && accessKeyId) {
    s3Client = new S3Client({ region, credentials: { accessKeyId, secretAccessKey } });
  } else {
    s3Client = new S3Client({ region });
  }
  const buildKey = (key: string) => {
    key = key.startsWith('/') ? key.slice(1) : key;
    const newPrefix = prefix?.startsWith('/') ? prefix.slice(1) : prefix;
    return `${newPrefix}/${key}`;
  };
  return {
    name: DRIVER_NAME,
    options,
    async hasItem(key) {
      const response = await s3Client.send(
        new HeadObjectCommand({
          Bucket: bucket,
          Key: buildKey(key),
        })
      );
      return response.$metadata.httpStatusCode === 200 && !response.DeleteMarker;
    },
    async getItem(key) {
      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: buildKey(key),
        })
      );
      if (response.$metadata.httpStatusCode === 200 && response.Body) {
        return await response.Body.transformToString();
      }
    },
    async getKeys() {
      const keys: string[] = [];
      let hasMore = false;
      let continuationToken: string | undefined;
      do {
        const response = await s3Client.send(
          new ListObjectsV2Command({
            Bucket: bucket,
            ContinuationToken: continuationToken,
          })
        );
        if (response.$metadata.httpStatusCode === 200) {
          if (response.Contents) {
            for (const item of response.Contents) {
              if (item.Key) {
                keys.push(item.Key);
              }
            }
          }
          hasMore = !!response.IsTruncated;
          if (hasMore) {
            continuationToken = response.ContinuationToken;
          }
        }
      } while (hasMore);
      return keys;
    },
  };
});
