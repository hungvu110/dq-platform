package dqplatform.client

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ParamsSpec extends AnyFunSuite with Matchers {

  private val values = Map("partition_end" -> "2026-10-18 00:00:00", "n" -> "10000", "batch" -> "dq_batch_x")

  test("substitute thay mọi ${x}, giữ nguyên ký tự đặc biệt trong giá trị") {
    Params.substitute("SELECT 1 FROM ${batch} WHERE t < TIMESTAMP '${partition_end}'", values) shouldBe
      "SELECT 1 FROM dq_batch_x WHERE t < TIMESTAMP '2026-10-18 00:00:00'"
    Params.substitute("a = '${v}'", Map("v" -> "$1\\x")) shouldBe "a = '$1\\x'"
  }

  test("substitute thiếu parameter -> lỗi nêu tên parameter") {
    val e = intercept[IllegalArgumentException](Params.substitute("x < ${missing}", values))
    e.getMessage should include("missing")
  }

  test("resolve bound số và {param}") {
    Params.resolve(Bound.Literal(0.97), values) shouldBe 0.97
    Params.resolve(Bound.Param("n"), values) shouldBe 10000.0
    intercept[IllegalArgumentException](Params.resolve(Bound.Param("partition_end"), values))
      .getMessage should include("không phải số")
  }

  test("requireAll: thiếu parameter khai báo -> lỗi nêu tên") {
    noException should be thrownBy Params.requireAll(Seq("n"), values)
    noException should be thrownBy Params.requireAll(Nil, Map("t" -> "2026-10-17 00:00:00.123+07:00", "n" -> "-1.5"))
    intercept[IllegalArgumentException](Params.requireAll(Seq("n", "partition_start"), values))
      .getMessage should include("partition_start")
  }

  test("requireAll: chặn giá trị có thể thoát khỏi string literal SQL (' và \\)") {
    // "\" + ") OR 1=1 --": dấu \ escape dấu ' đóng chuỗi, phần sau rơi ra ngoài literal
    Seq("a' OR 1=1 --", "\\", ") OR 1=1 --;", "x\ny", "2026-10-17\n").foreach { bad =>
      withClue(bad)(intercept[IllegalArgumentException](Params.requireAll(Nil, Map("x" -> bad)))
        .getMessage should include("không cho phép"))
    }
  }

  test("ResolvedAssertion: hàm kiểm và ngưỡng") {
    val mostly = Params.resolve(Assertion(">=", Some(Bound.Literal(0.97)), None, None), values)
    mostly.fn(0.98) shouldBe true
    mostly.fn(0.96) shouldBe false
    mostly.threshold shouldBe Some(0.97)
    val between = Params.resolve(Assertion("between", None, Some(Bound.Literal(1)), Some(Bound.Literal(2))), values)
    (between.fn(1.0), between.fn(2.0), between.fn(2.1)) shouldBe ((true, true, false))
    Params.resolve(Assertion("<=", Some(Bound.Literal(5)), None, None), values).threshold shouldBe None
  }
}
