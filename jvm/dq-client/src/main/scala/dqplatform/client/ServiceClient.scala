package dqplatform.client

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.{URLEncoder => Enc}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration

/**
 * Lỗi gọi DQ service. status = None: mạng/timeout; Some(code): HTTP != 2xx.
 * 4xx là lỗi cấu hình/hợp đồng (sai token, chưa có suite, payload bị từ chối) chứ không phải "service sập".
 */
final class ServiceException(message: String, val status: Option[Int] = None, cause: Throwable = null)
  extends RuntimeException(message, cause) {
  def isClientError: Boolean = status.exists(s => s >= 400 && s < 500)
}

/** Hai lời gọi duy nhất ra mạng, chỉ driver gọi. Tách trait để test DQClient không cần HTTP. */
trait ServiceApi {
  /** GET /api/v1/suites/{dataset} -> {dataset, version, suite, policy} */
  def getSuite(dataset: String): JsonNode
  /** POST /api/v1/runs -> {run_id, verdict, reasons, dimensions} */
  def postRun(payload: JsonNode): JsonNode
}

final class HttpServiceApi(baseUrl: String, token: String, timeout: Duration) extends ServiceApi {
  private val mapper = new ObjectMapper()
  private val base = baseUrl.stripSuffix("/")
  // HTTP/1.1: mặc định HttpClient thử nâng lên h2c (header Upgrade) với http://, uvicorn xử lý sai
  // request có body khi gặp header này -> POST /runs mất body (HTTP 400/422)
  private val http = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_1_1)
    .connectTimeout(timeout)
    .build()

  override def getSuite(dataset: String): JsonNode =
    send(request(s"/api/v1/suites/${Enc.encode(dataset, UTF_8).replace("+", "%20")}").GET().build())

  override def postRun(payload: JsonNode): JsonNode =
    send(request("/api/v1/runs")
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(payload)))
      .build())

  private def request(path: String): HttpRequest.Builder =
    HttpRequest.newBuilder(URI.create(base + path))
      .timeout(timeout)
      .header("Authorization", s"Bearer $token")

  private def send(req: HttpRequest): JsonNode = {
    val resp =
      try http.send(req, HttpResponse.BodyHandlers.ofString())
      catch { case e: Exception => throw new ServiceException(s"${req.method} ${req.uri}: ${e.getMessage}", cause = e) }
    if (resp.statusCode / 100 != 2)
      throw new ServiceException(s"${req.method} ${req.uri}: HTTP ${resp.statusCode} ${resp.body.take(500)}",
        status = Some(resp.statusCode))
    mapper.readTree(resp.body)
  }
}
