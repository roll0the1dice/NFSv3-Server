package com.example.netclient.httpclient;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.file.FileSystem;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.core.http.HttpClientAgent;
import io.vertx.reactivex.core.http.HttpClientRequest;
import io.vertx.reactivex.core.http.HttpClientResponse;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Data
@AllArgsConstructor
@Builder
public class UpDownHttpClient {
  private String accessKey;
  private String secretKey;
  private String region;
  private String service;
  private AwsSignerCreater.SignerType signerType;
  private HttpClient client;

  public Single<Buffer> get(String targetUrl) throws URISyntaxException {
    URI uri = new URI(targetUrl);
    String host = uri.getHost();
    String resourcePath = uri.getPath();
    AwsSigner awsSigner = AwsSignerCreater.createSigner(signerType, accessKey, secretKey, region, service);

    Single<HttpClientRequest> request = client.rxRequest(HttpMethod.GET, 80, host, resourcePath);
    Single<Buffer> responseBuffer = request.flatMap(httpClientRequest -> {
        Map<String, String> pseudoHeaders = new HashMap<>();
        String authorization = awsSigner.calculateAuthorization(String.valueOf(HttpMethod.GET), targetUrl, pseudoHeaders, null);

        for (Map.Entry<String, String> header : pseudoHeaders.entrySet()) {
          httpClientRequest.putHeader(header.getKey(), header.getValue());
        }
        httpClientRequest.putHeader("Authorization", authorization);
        if (signerType == AwsSignerCreater.SignerType.AWS_V2) {
          httpClientRequest.putHeader("Date", awsSigner.getAmzDate());
        } else {
          httpClientRequest.putHeader("x-amz-date", awsSigner.getAmzDate());
        }

        return httpClientRequest.rxSend();
      })
      .flatMap(HttpClientResponse::rxBody)
      .subscribeOn(Schedulers.io());

    return responseBuffer;
  }

  public Single<Buffer> put(String targetUrl, Flowable<Buffer> fileBodyFlowable) throws URISyntaxException {
    URI uri = new URI(targetUrl);
    String host = uri.getHost();
    String resourcePath = uri.getPath();
    AwsSigner awsSigner = AwsSignerCreater.createSigner(signerType, accessKey, secretKey, region, service);

    Single<HttpClientRequest> requestSingle = client.rxRequest(HttpMethod.PUT, 80, host, resourcePath);
    Single<Buffer> responseBodySingle = requestSingle.flatMap(httpClientRequest -> {
        Map<String, String> pseudoHeaders = new HashMap<>();
        pseudoHeaders.put("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

        String authorization = awsSigner.calculateAuthorization(String.valueOf(HttpMethod.PUT), targetUrl, pseudoHeaders, null);

        for (Map.Entry<String, String> entry : pseudoHeaders.entrySet()) {
          httpClientRequest.putHeader(entry.getKey(), entry.getValue());
        }
        httpClientRequest.putHeader("Authorization", authorization);
        if (signerType == AwsSignerCreater.SignerType.AWS_V2) {
          httpClientRequest.putHeader("Date", awsSigner.getAmzDate());
        } else {
          httpClientRequest.putHeader("x-amz-date", awsSigner.getAmzDate());
        }

        return httpClientRequest.rxSend(fileBodyFlowable);
      })
      .flatMap(httpClientResponse -> {
        System.out.println("Upload successful! Server response status: " + httpClientResponse.statusCode() + " " + httpClientResponse.statusMessage());
        if (httpClientResponse.statusCode() >= 200 && httpClientResponse.statusCode() < 300) {
          return httpClientResponse.rxBody();
        } else {
          return httpClientResponse.rxBody();
        }
      });

    return responseBodySingle;
  }

  public static void main(String[] args) throws URISyntaxException {
    Vertx vertx = Vertx.vertx();
    FileSystem fs = vertx.fileSystem();
    HttpClient client = vertx.createHttpClient();

    String host = "172.20.123.124";
    String bucket = "mybucket";
    String key = "hello";
    String targetUrl = String.format("http://%s/%s/%s", host, bucket, key);
    String resourcePath = String.format("/%s/%s", bucket, key);
    String accessKey = "MAKIC8SGQQS8424CZT07";
    String secretKey = "tDFRk9bGzS0j5JXPrdS0qSXL40zSn3xbBRZsPfEH";
    String regionNmae = "us-east-1";
    String serviceName = "s3";

    UpDownHttpClient upDownHttpClient = UpDownHttpClient.builder()
      .client(client)
      .accessKey(accessKey)
      .secretKey(secretKey)
      .region(regionNmae)
      .service(serviceName)
      .signerType(AwsSignerCreater.SignerType.AWS_V4)
      .build();

//    Disposable subscribe = fs.rxOpen("test_file.txt", new OpenOptions().setRead(true))
//      .flatMap(asyncFile -> {
//        log.info("asyncFile: {}", asyncFile.size());
//
//        return upDownHttpClient.put(targetUrl, asyncFile.toFlowable())
//          .doFinally(asyncFile::close);
//      })
//      .doOnSuccess(responseBody -> log.info("Upload processing completed successfully, Response Body {}", responseBody.toString()))
//      .doOnError(error -> log.info("Operation failed: {}", error))
//      .ignoreElement()
//      .subscribe(
//        () -> log.info("Upload and close operation COMPLETED."),
//        error -> log.error("Upload and close operation FAILED.", error)
//      );

    Single<Buffer> bufferSingle = upDownHttpClient.get(targetUrl);
    int datalength = bufferSingle
      .flatMap(buffer -> {
        return Single.just(buffer.length());
      })
      .blockingGet();

    System.out.println(datalength);

    //subscribe.dispose();
  }
}
