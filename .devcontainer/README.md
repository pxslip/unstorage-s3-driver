# S3 Driver for Unstorage

(Unstorage)[https://nitro.unjs.io/guide/storage] allows for custom drivers. S3 seems like a possible option.

## Usage

```ts
import { createStorage } from "unstorage";
import { s3StorageDriver } from "@pxslips/unstorage-s3-driver";

const storage = createStorage({
	driver: s3StorageDriver(),
});
```
