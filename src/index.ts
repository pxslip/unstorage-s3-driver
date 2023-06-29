import { defineDriver } from 'unstorage';
import { createRequiredError } from 'unstorage/drivers/utils/index.mjs';

import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  DeleteObjectCommand,
  DeleteObjectCommandInput,
  PutObjectCommandInput,
  PutObjectCommand,
} from '@aws-sdk/client-s3';

const DRIVER_NAME = 's3';

export interface S3Options {
  bucket: string;
  prefix?: string;
  region?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
}

export interface SetItemOptions {
  meta?: Record<string, string>;
  hasBody?: boolean;
}

let s3Client: S3Client;

export const s3StorageDriver = defineDriver<S3Options>((options) => {
  const { bucket, prefix, region, accessKeyId, secretAccessKey } = options;
  if (!bucket) {
    throw createRequiredError(DRIVER_NAME, 'bucket');
  }
  if (secretAccessKey && accessKeyId) {
    s3Client = new S3Client({ region, credentials: { accessKeyId, secretAccessKey } });
  } else {
    //TODO: does this throw an error? If so should I catch it and rethrow?
    s3Client = new S3Client({ region });
  }

  const buildKey = (key: string) => {
    key = key.startsWith('/') ? key.slice(1) : key;
    const newPrefix = prefix?.startsWith('/') ? prefix.slice(1) : prefix;
    return `${newPrefix}/${key}`;
  };

  const headObject = async (key: string) => {
    return await s3Client.send(
      new HeadObjectCommand({
        Bucket: bucket,
        Key: buildKey(key),
      })
    );
  };
  /**
   * //TODO: Is this sufficient indication that the item exists (or not)?
   * @param key The key of the item to test for
   * @returns true if the response returns success, and the object is not marked as deleted
   */
  const hasItem = async (key) => {
    const response = await headObject(key);
    return response.$metadata.httpStatusCode === 200 && !response.DeleteMarker;
  };
  /**
   *
   * @param key the key of the item stored in S3 to get
   * @returns An uint8array of the data returned, i.e. an array of the bytes of the object
   */
  const getItemRaw = async (key) => {
    const response = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: buildKey(key),
      })
    );
    if (response.$metadata.httpStatusCode === 200 && response.Body) {
      return await response.Body.transformToByteArray();
    }
  };
  /**
   *
   * @param key The key of the item to get
   * @returns The string representation of this object
   */
  const getItem = async (key) => {
    const item = await getItemRaw(key);
    if (item) {
      return new TextDecoder().decode(item);
    }
  };
  /**
   *
   * @returns a list of all keys in the bucket, may require multiple queries as only 1000 keys are returned per query
   */
  const getKeys = async () => {
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
  };
  /**
   * TODO: Handle getting any user defined metadata from the item
   * @param key The key of the item whose metadata we want
   * @returns An object with the last modified `mtime` and, eventually, any user-defined metadata
   */
  const getMeta = async (key) => {
    const response = await headObject(key);
    //TODO: handle custom metadata set when the user creates, or updates the item, or when they specifically set the metadata
    return {
      mtime: response.LastModified,
    };
  };
  /**
   * Removes all items from the S3 bucket
   */
  const clear = async () => {
    const keys = await getKeys();
    const errors = new Map<string, { Key?: string; VersionId?: string; Code?: string; Message?: string }>();
    while (keys.length > 0) {
      const response = await s3Client.send(
        new DeleteObjectsCommand({
          Bucket: bucket,
          Delete: {
            Objects: keys.splice(0, 999).map((key) => {
              return { Key: key };
            }),
            Quiet: true,
          },
        })
      );
      if (response.$metadata.httpStatusCode === 200) {
        for (const error of response.Errors ?? []) {
          errors.set(`${error.Key}:${error.VersionId}`, error);
        }
      }
    }
  };
  /**
   * Uploads a new item to an S3 bucket, this item must be a string
   * @param key the key under which to store this item
   * @param value the body of the object to upload
   * @param opts.meta any metadata to add to the object
   */
  const setItemRaw = async (key, value: PutObjectCommandInput['Body'], { meta, hasBody }: SetItemOptions) => {
    const input: PutObjectCommandInput = {
      Bucket: bucket,
      Key: key,
    };
    if (hasBody) {
      input.Body = value;
    }
    if (meta) {
      const cleanMeta: Record<string, string> = {};
      for (let key in meta) {
        const value = meta[key];
        if (!key.startsWith('x-amz-meta-')) {
          key = `x-amz-meta-${key}`;
        }
        cleanMeta[key] = value;
      }
      input.Metadata = cleanMeta;
    }
    await s3Client.send(new PutObjectCommand(input));
  };

  /**
   * Uploads a new item to an S3 bucket, this item can be any type of content
   * @param key the key under which to store this item
   */
  const setItem = async (key, value, options: SetItemOptions) => {
    await setItemRaw(key, value, options);
  };

  /**
   * Removes an item from the bucket
   * @param key The key of the item to remove from the bucket
   * @param opts.version If bucket versioning is enabled and a specific version should be removed
   */
  const removeItem = async (key, { version }: { version?: string }) => {
    const input: DeleteObjectCommandInput = {
      Bucket: bucket,
      Key: key,
    };
    if (version) {
      input.VersionId = version;
    }
    await s3Client.send(new DeleteObjectCommand(input));
  };

  const dispose = async () => {
    s3Client.destroy();
  };
  return {
    name: DRIVER_NAME,
    options,
    hasItem,
    getItem,
    getItemRaw,
    setItem,
    setItemRaw,
    removeItem,
    getMeta,
    getKeys,
    clear,
    dispose,
  };
});
