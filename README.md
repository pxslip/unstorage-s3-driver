# S3 Driver for Unstorage

[Unstorage](https://nitro.unjs.io/guide/storage) allows for custom drivers. S3 seems like a possible option.

## Usage

```ts
import { createStorage } from 'unstorage';
import { s3StorageDriver } from '@pxslips/unstorage-s3-driver';

const storage = createStorage({
  driver: s3StorageDriver(),
});
```

## To Do

- [] Add options to all commands to better support more complex S3 options
- [] Return the user-defined metadata from `getMeta`
- [] Investigate implementing `watch`
- [] Investigate effort to implement using the http API instead of sdk per [#11](https://github.com/unjs/unstorage/issues/11)

## Ideas for usage with Nuxt and/or ISR

Two options worth considering wrt this driver and Nuxt ISR:

- When storing, use the TTL as an ETag/Cache timeout when putting an object to S3, then use that as a part of the call logic, if the object has expired re-run the SSR lambda to regenerate it. Otherwise respond from S3.

- Set an extremely long TTL, invalidate the cache object using a publish webhook or other build script from the content source to invalidate the correct paths.
  - What to do about search and other pages that shouldn't have a long TTL? Purely client side?
