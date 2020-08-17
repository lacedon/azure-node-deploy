import path from 'path';

import { BlobServiceClient, ContainerClient, BlockBlobClient } from '@azure/storage-blob';
import mime from 'mime-types';

import deploy, { CFile, CDir, FsFileDesc, FsDirDesc } from './deploy';

async function getDirFiles(dirClient: ContainerClient): Promise<string[]> {
  const files: string[] = [];
  for await (const { name } of dirClient.listBlobsFlat()) {
    files.push(name);
  }
  return files;
}

export default function deployBlob(payload: {
  connectionString: string;
  storageName?: string;
  from?: string;
  to?: string;
}): Promise<void> {
  const serviceClient: BlobServiceClient = BlobServiceClient.fromConnectionString(payload.connectionString);
  const storageClient: ContainerClient = serviceClient.getContainerClient(payload.storageName);
  const rootFSDir = payload.from;

  return deploy<BlockBlobClient, ContainerClient>({
    rootCDir: storageClient,
    rootFSDir,
    getCDirFiles: getDirFiles,
    async getCFile(payload: {
      fileName: string;
      dir: CDir<ContainerClient>;
    }): Promise<CFile<BlockBlobClient> | CDir<ContainerClient>> {
      return {
        type: 'file',
        path: path.join(payload.dir.path, payload.fileName),
        client: payload.dir.client.getBlockBlobClient(payload.fileName),
      };
    },
    async deleteCFile(payload: { file: CFile<BlockBlobClient> | CDir<ContainerClient> }): Promise<void> {
      if (payload.file.type === 'dir') return;
  
      await payload.file.client.delete();
    },
    async uploadFSFile(payload: { file: FsFileDesc<BlockBlobClient> | FsDirDesc<ContainerClient> }): Promise<void> {
      if (payload.file.type === 'dir') return;
  
      const filePath = payload.file.path.slice(rootFSDir.length + 1);
      storageClient.getBlockBlobClient(filePath).uploadFile(payload.file.path, {
        blobHTTPHeaders: { blobContentType: mime.lookup(payload.file.path) || '' },
      });
    },
    async getClient(payload: {
      dir: FsDirDesc<ContainerClient>;
      file: FsDirDesc<null> | FsFileDesc<null>;
      fileName: string;
    }): Promise<ContainerClient | BlockBlobClient> {
      return payload.dir.client.getBlockBlobClient(payload.fileName);
    },
  });
}
