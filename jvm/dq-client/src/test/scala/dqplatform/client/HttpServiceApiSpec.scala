package dqplatform.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class HttpServiceApiSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val mapper = new ObjectMapper()
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
  private val lastRequest = new AtomicReference[(String, String, String, String)]() // method, path, auth, body
  private val lastUpgrade = new AtomicReference[String]()

  private def reply(ex: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(UTF_8)
    ex.sendResponseHeaders(status, bytes.length.toLong)
    ex.getResponseBody.write(bytes)
    ex.close()
  }

  override def beforeAll(): Unit = {
    server.createContext("/", (ex: HttpExchange) => {
      val body = new String(ex.getRequestBody.readAllBytes(), UTF_8)
      lastRequest.set((ex.getRequestMethod, ex.getRequestURI.getRawPath,
        ex.getRequestHeaders.getFirst("Authorization"), body))
      lastUpgrade.set(ex.getRequestHeaders.getFirst("Upgrade"))
      ex.getRequestURI.getPath match {
        case "/api/v1/suites/silver.transactions" => reply(ex, 200, """{"version": 2, "suite": {}}""")
        case "/api/v1/runs"                        => reply(ex, 200, """{"verdict": "PASS", "reasons": []}""")
        case "/api/v1/suites/broken"               => reply(ex, 500, "boom")
        case _                                     => reply(ex, 404, """{"detail": "no suite"}""")
      }
    })
    server.start()
  }

  override def afterAll(): Unit = server.stop(0)

  private def api(timeout: Duration = Duration.ofSeconds(5)) =
    new HttpServiceApi(s"http://127.0.0.1:${server.getAddress.getPort}/", "tok", timeout)

  test("GET suite: gửi Bearer token, đúng path") {
    api().getSuite("silver.transactions").get("version").asInt shouldBe 2
    val (method, path, auth, _) = lastRequest.get()
    (method, path, auth) shouldBe (("GET", "/api/v1/suites/silver.transactions", "Bearer tok"))
  }

  test("tên dataset được encode trong path") {
    intercept[ServiceException](api().getSuite("a b/c"))
    lastRequest.get()._2 shouldBe "/api/v1/suites/a%20b%2Fc"
  }

  test("POST run: gửi JSON, đọc verdict") {
    val payload = mapper.createObjectNode().put("run_id", "r1")
    api().postRun(payload).get("verdict").asText shouldBe "PASS"
    val (method, path, _, body) = lastRequest.get()
    (method, path) shouldBe (("POST", "/api/v1/runs"))
    mapper.readTree(body).get("run_id").asText shouldBe "r1"
  }

  test("không gửi Upgrade: h2c (uvicorn làm mất body của POST khi có header này)") {
    api().postRun(mapper.createObjectNode().put("run_id", "r2"))
    lastUpgrade.get() shouldBe null
  }

  test("HTTP khác 2xx hoặc không kết nối được -> ServiceException") {
    val e500 = intercept[ServiceException](api().getSuite("broken"))
    e500.getMessage should include("HTTP 500")
    e500.isClientError shouldBe false
    val e404 = intercept[ServiceException](api().getSuite("unknown"))
    (e404.status, e404.isClientError) shouldBe ((Some(404), true))
    val dead = new HttpServiceApi("http://127.0.0.1:1", "tok", Duration.ofSeconds(1))
    intercept[ServiceException](dead.getSuite("x")).status shouldBe None
  }
}
