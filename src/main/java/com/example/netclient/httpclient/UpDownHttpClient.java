package com.example.netclient.httpclient;

import com.example.netclient.model.DTO.CompleteMultipartUpload;
import com.example.netclient.model.DTO.InitiateMultipartUploadResult;
import com.example.netclient.model.DTO.Part;
import com.example.netclient.model.DTO.PartInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.net.ProxyOptions;
import io.vertx.core.net.ProxyType;
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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

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

  public Single<Buffer> delete(String targetUrl) throws URISyntaxException {
    URI uri = new URI(targetUrl);
    String host = uri.getHost();
    String resourcePath = uri.getPath();
    AwsSigner awsSigner = AwsSignerCreater.createSigner(signerType, accessKey, secretKey, region, service);

    Single<HttpClientRequest> request = client.rxRequest(HttpMethod.DELETE, 80, host, resourcePath);
    Single<Buffer> responseBuffer = request.flatMap(httpClientRequest -> {
        Map<String, String> pseudoHeaders = new HashMap<>();
        String authorization = awsSigner.calculateAuthorization(String.valueOf(HttpMethod.DELETE), targetUrl, pseudoHeaders, null);

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

  public Single<Buffer> post(String targetUrl, Flowable<Buffer> fileBodyFlowable, long fileSize) throws URISyntaxException {
    URI uri = new URI(targetUrl);
    String host = uri.getHost();
    String resourcePath = uri.getPath();
    String query = uri.getQuery();

    AwsSigner awsSigner = AwsSignerCreater.createSigner(signerType, accessKey, secretKey, region, service);

    Single<HttpClientRequest> requestSingle = client.rxRequest(HttpMethod.POST, 80, host, resourcePath + "?" + query);
    Single<Buffer> responseBodySingle = requestSingle.flatMap(httpClientRequest -> {
        Map<String, String> pseudoHeaders = new HashMap<>();
        pseudoHeaders.put("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
        pseudoHeaders.put("Content-Length", String.valueOf(fileSize));

        String authorization = awsSigner.calculateAuthorization(String.valueOf(HttpMethod.POST), targetUrl, pseudoHeaders, null);

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
          return Single.just(Buffer.buffer(httpClientResponse.headers().get("x-amz-next-append-position")));
        } else {
          log.info("x-amz-next-append-position: {}", httpClientResponse.headers().get("x-amz-next-append-position"));
          return httpClientResponse.rxBody();
        }
      });

    return responseBodySingle;
  }

  public Single<Buffer> initMultiPartUpload(String host, String resourcePath) throws URISyntaxException {
    if (client == null) {
      throw new IllegalArgumentException("httpClient is null");
    }

    String url = String.format("http://%s%s?uploads=", host, resourcePath);

    Single<HttpClientRequest> requestSingle = client.rxRequest(HttpMethod.POST, url);
    Single<Buffer> responseBodySingle = requestSingle.flatMap(httpClientRequest -> {
        System.out.println("HttpClientRequest obtained. Sending file content...");
        Map<String, String> pseudoHeaders = new HashMap<>();
        pseudoHeaders.put("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

        AwsSigner awsSigner = AwsSignerCreater.createSigner(signerType, accessKey, secretKey, region, service);
        String authorization = awsSigner.calculateAuthorization(String.valueOf(HttpMethod.POST), url, pseudoHeaders, null);

        for (Map.Entry<String, String> entry : pseudoHeaders.entrySet()) {
          httpClientRequest.putHeader(entry.getKey(), entry.getValue());
        }
        httpClientRequest.putHeader("Authorization", authorization);
        if (signerType == AwsSignerCreater.SignerType.AWS_V2) {
          httpClientRequest.putHeader("Date", awsSigner.getAmzDate());
        } else {
          httpClientRequest.putHeader("x-amz-date", awsSigner.getAmzDate());
        }

        return httpClientRequest.rxSend();
      })
      .flatMap(httpClientResponse -> { // Transform HttpClientResponse to its body
        System.out.println("Upload successful! Server response status: " + httpClientResponse.statusCode() + " " + httpClientResponse.statusMessage());
        // Check status code before attempting to get body as a "success"
        if (httpClientResponse.statusCode() >= 200 && httpClientResponse.statusCode() < 300) {
          return httpClientResponse.rxBody(); // This returns Single<Buffer>
        } else {
          // Non-successful HTTP status. We want the outer Single to fail.
          // We'll try to get the body to include it in the error message.
          return httpClientResponse.rxBody();
        }
      });

    return responseBodySingle;
  }


  public Single<Buffer> completeMultiPartUpload(String host, String resourcePath, String uploadId, List<Part> MultiPartUploadList) throws URISyntaxException, JsonProcessingException {
    String url = String.format("http://%s%s?uploadId=%s", host, resourcePath, uploadId);

    // 构造XML字面量字符串
    XmlMapper xmlMapper = new XmlMapper();
    xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, false);

    CompleteMultipartUpload completeMultipartUpload = new CompleteMultipartUpload();
    List<Part> partList = MultiPartUploadList.stream().sorted(Comparator.comparing(Part::getPartNumber))
      .collect(Collectors.toList());
    completeMultipartUpload.setParts(partList);

    String xmlLiteral = xmlMapper.writeValueAsString(completeMultipartUpload);

    Single<HttpClientRequest> requestSingle = client.rxRequest(HttpMethod.POST, url);

    Single<Buffer> responseBodySingle = requestSingle.flatMap(httpClientRequest -> {
        System.out.println("HttpClientRequest obtained. Sending file content...");
        Map<String, String> pseudoHeaders = new HashMap<>();
        pseudoHeaders.put("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

        AwsSigner awsSigner = AwsSignerCreater.createSigner(signerType, accessKey, secretKey, region, service);
        String authorization = awsSigner.calculateAuthorization(String.valueOf(HttpMethod.POST), url, pseudoHeaders, null);

        for (Map.Entry<String, String> entry : pseudoHeaders.entrySet()) {
          httpClientRequest.putHeader(entry.getKey(), entry.getValue());
        }
        httpClientRequest.putHeader("Authorization", authorization);
        if (signerType == AwsSignerCreater.SignerType.AWS_V2) {
          httpClientRequest.putHeader("Date", awsSigner.getAmzDate());
        } else {
          httpClientRequest.putHeader("x-amz-date", awsSigner.getAmzDate());
        }

        return httpClientRequest.rxSend(Buffer.buffer(xmlLiteral));
      })
      .flatMap(httpClientResponse -> { // Transform HttpClientResponse to its body
        System.out.println("Upload successful! Server response status: " + httpClientResponse.statusCode() + " " + httpClientResponse.statusMessage());
        // Check status code before attempting to get body as a "success"
        if (httpClientResponse.statusCode() >= 200 && httpClientResponse.statusCode() < 300) {
          return httpClientResponse.rxBody(); // This returns Single<Buffer>
        } else {
          // Non-successful HTTP status. We want the outer Single to fail.
          // We'll try to get the body to include it in the error message.
          return httpClientResponse.rxBody();
        }
      })
      .doFinally(() -> {
        System.out.println("Upload finished or failed. Closing file: " + url);
      });

    return responseBodySingle;
  }

  public Single<Buffer> performMultiPartUpload(String host, String resourcePath, Flowable<Buffer> fileBodyFlowable) throws URISyntaxException, JsonProcessingException {
    try {
      return this.initMultiPartUpload(host, resourcePath)
        .flatMapPublisher(buffer -> {
          String initResponseXmlText = buffer.toString();

          XmlMapper xmlMapper = new XmlMapper();
          InitiateMultipartUploadResult initiateMultipartUploadResult
            = xmlMapper.readValue(initResponseXmlText, InitiateMultipartUploadResult.class);
          if (initiateMultipartUploadResult == null || initiateMultipartUploadResult.getUploadId() == null) {
            throw new IllegalArgumentException("InitiateMultipartUploadResult is null");
          }

          String uploadId = initiateMultipartUploadResult.getUploadId();
          log.debug("INFO: Multipart upload initiated. UploadId: {}", uploadId);
          List<PartInfo> partsToUpload = new ArrayList<>(); // this.calculatePartInfo(fileSize, transferConfig.getMultiPartChunkSize(), uploadId);

          return Flowable.fromIterable(partsToUpload)
            .flatMapSingle(partInfo -> {
              return this.uploadPartWorkers(host, resourcePath, uploadId, partInfo, fileBodyFlowable);
            });
        })
        .toList()
        .flatMap(parts -> {
          //String uploadId = parts.get(0).getUploadId();
          String uploadId = parts.get(0).getUploadId();
          return this.completeMultiPartUpload(host, resourcePath, uploadId, parts);
        });
      //log.debug("SUCCESS: Multipart upload completed. Final ETag: {}", finalResponse);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public Single<Buffer> upLoadPart(
    String host,
    String resourcePath,
    Integer partNumber,
    String uploadId,
    Flowable<Buffer> payLoad) throws URISyntaxException {
    String targetUrl = String.format("http://%s%s?uploadId=%s&partNumber=%s", host, resourcePath, uploadId, partNumber);

    AwsSigner awsSigner = AwsSignerCreater.createSigner(signerType, accessKey, secretKey, region, service);
    Single<HttpClientRequest> requestSingle = client.rxRequest(HttpMethod.PUT, targetUrl);

    Single<Buffer> responseBodySingle = requestSingle.flatMap(httpClientRequest -> {
        System.out.println("HttpClientRequest obtained. Sending file content...");

        Map<String, String> pseudoHeaders = new HashMap<>();
        String authorization = awsSigner.calculateAuthorization(String.valueOf(HttpMethod.PUT), targetUrl, pseudoHeaders, null);

        for (Map.Entry<String, String> header : pseudoHeaders.entrySet()) {
          httpClientRequest.putHeader(header.getKey(), header.getValue());
        }
        httpClientRequest.putHeader("Authorization", authorization);
        if (signerType == AwsSignerCreater.SignerType.AWS_V2) {
          httpClientRequest.putHeader("Date", awsSigner.getAmzDate());
        } else {
          httpClientRequest.putHeader("x-amz-date", awsSigner.getAmzDate());
        }

        return httpClientRequest.rxSend(payLoad);
      })
      .flatMap(httpClientResponse -> { // Transform HttpClientResponse to its body
        // Check status code before attempting to get body as a "success"
        if (httpClientResponse.statusCode() >= 200 && httpClientResponse.statusCode() < 300) {

          String ETag = httpClientResponse.getHeader("ETag");

          return Single.just(Buffer.buffer(ETag.getBytes(StandardCharsets.UTF_8))); // This returns Single<Buffer>
        } else {
          // Non-successful HTTP status. We want the outer Single to fail.
          // We'll try to get the body to include it in the error message.
          return httpClientResponse.rxBody();
        }
      });

    return responseBodySingle;
  }

  public Single<Part> uploadPartWorkers(String host, String resourcePath, String upLoadId, PartInfo partInfo, Flowable<Buffer> flowableBuffer) throws URISyntaxException {
    int partNumber = partInfo.getPartNumber();
    Long offset = partInfo.getOffset();
    int size = partInfo.getSize();

    Single<Part> Etags = this.upLoadPart(host, resourcePath, partNumber, upLoadId, flowableBuffer)
      .flatMap(buffer -> {
        String eTag = buffer.toString();
        Part part = Part.builder().partNumber(partNumber).eTag(eTag).uploadId(upLoadId).build();

        return Single.just(part);
      });

    return Etags;
  }


  public List<PartInfo> calculatePartInfo(Long fileSize, Integer chunkSize, String uploadId) {
    long numParts = (long) Math.ceil((double) fileSize /chunkSize);
    List<PartInfo> partInfos = new ArrayList<>();

    for (int i = 0; i < numParts; i++) {
      int partNumber = i + 1;
      long offset = (long) i * chunkSize;
      int size = (int) Math.min(chunkSize, fileSize - offset);
      partInfos.add(PartInfo.builder()
        .partNumber(partNumber)
        .offset(offset)
        .size(size)
        .uploadId(uploadId)
        .build());
    }
    return partInfos;
  }


  public void uploadFile(String targetUrl, Flowable<Buffer> payLoadFlowable) throws URISyntaxException, JsonProcessingException {
    URI uri = new URI(targetUrl);
    String host = uri.getHost();
    String resourcePath = uri.getPath();

    this.performMultiPartUpload(host, resourcePath, payLoadFlowable).subscribe(
      buffer -> {
        System.out.println("response: " + buffer.toString());
      },
      Throwable::printStackTrace
    );
  }

  public static void main(String[] args) throws URISyntaxException {
    Vertx vertx = Vertx.vertx();
    FileSystem fs = vertx.fileSystem();
    HttpClient client = vertx.createHttpClient();

    String host = "172.20.123.124";
    String bucket = "mybucket";
    String key = "hello";
    //String targetUrl = String.format("http://%s/%s/%s", host, bucket, key);
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


    Flowable<Buffer> b1 = fs.rxOpen("test_file.txt", new OpenOptions().setRead(true)).flatMapPublisher(asyncFile -> {
      return asyncFile.toFlowable();
    });

    Flowable<Buffer> b2 = fs.rxOpen("test_file.txt", new OpenOptions().setRead(true)).flatMapPublisher(asyncFile -> {
      return asyncFile.toFlowable();
    });

    List<Flowable<Buffer>> bList = List.of(b1, b2);

    Flowable<Buffer> bs = Flowable.concat(bList);
//        .flatMap(asyncFile -> {
//          log.info("asyncFile: {}");
//          String targetUrl = String.format("http://%s/%s/%s?append=&position=%s", host, bucket, key, 0);
//          log.info("targetUrl: " + targetUrl);
//
//          return
//        });

    String targetUrl = String.format("http://%s/%s/%s?append=&position=%s", host, bucket, key, 0);
    long filesize = 66560 * 2;
    upDownHttpClient.post(targetUrl, bs, filesize).doOnSuccess(buffer -> {
      System.out.println("response: " + buffer.toString());
    })
      .subscribe();

//    response.flatMap(
//      buffer -> {
//        String position = buffer.toString();
//        return fs.rxOpen("test_file.txt", new OpenOptions().setRead(true))
//          .flatMap(asyncFile -> {
//            log.info("asyncFile: {}");
//            String targetUrl = String.format("http://%s/%s/%s?append=&position=%s", host, bucket, key, position);
//            log.info("targetUrl: " + targetUrl);
//
//            return upDownHttpClient.post(targetUrl, asyncFile.toFlowable(), asyncFile.sizeBlocking())
//              .doFinally(asyncFile::close);
//          });
//      }
//    )
//      .subscribe();

//    Single<Buffer> bufferSingle = upDownHttpClient.delete(targetUrl);
//    int datalength = bufferSingle
//      .flatMap(buffer -> {
//        return Single.just(buffer.length());
//      })
//      .blockingGet();
//
//    System.out.println(datalength);

    //subscribe.dispose();
  }
}
