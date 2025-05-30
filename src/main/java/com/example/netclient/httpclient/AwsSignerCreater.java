package com.example.netclient.httpclient;

public class AwsSignerCreater {
  public enum SignerType {
    AWS_V2,
    AWS_V4,
    AWS_V4_CHUNKED_UPLOAD
  }
  public static AwsSigner createSigner(SignerType signerType, String accessKeyId, String secretAccessKey, String region, String service) {
    switch (signerType) {
      case AWS_V2:
        return new AwsV2Signer(accessKeyId, secretAccessKey);
      case AWS_V4:
        return new AwsV4Signer(accessKeyId, secretAccessKey, region, service);
      case AWS_V4_CHUNKED_UPLOAD:
        return new AwsV4SignerForChunkedUpload(accessKeyId, secretAccessKey, region, service);
      default:
        throw new IllegalArgumentException("Unsupported signer type: " + signerType);
    }
  }
}
