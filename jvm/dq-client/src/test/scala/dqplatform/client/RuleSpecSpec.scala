package dqplatform.client

import com.fasterxml.jackson.databind.ObjectMapper
import dqplatform.client.CheckSpec._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

class RuleSpecSpec extends AnyFunSuite with Matchers {

  private val mapper = new ObjectMapper()

  /** Suite thật trong repo, dạng body của PUT /suites: {dataset, policy, suite}. */
  private def realSuiteJson: String =
    new String(Files.readAllBytes(Paths.get("../../suites/silver_transactions.json")), "UTF-8")

  private def suiteWith(rules: String, params: String = "{}"): String =
    s"""{"format": "dq-rules/v2", "parameters": $params, "rules": [$rules]}"""

  test("parse suite silver.transactions trong repo: 21 rule, đủ 6 dimension") {
    val suite = RuleSpec.parseSuite(mapper.readTree(realSuiteJson).get("suite"))
    suite.rules should have size 21
    suite.parameters should contain theSameElementsAs Seq("partition_start", "partition_end", "source_row_count")
    suite.rules.map(_.dimension).toSet shouldBe RuleSpec.Dimensions
    suite.rules.find(_.id == "row_count_reconcile").get.check shouldBe
      Size(Assertion("==", Some(Bound.Param("source_row_count")), None, None))
    suite.rules.filter(_.rowLevel).map(_.id) should contain allOf ("msisdn_format", "event_time_in_partition")
  }

  test("parse đủ các kind") {
    val suite = RuleSpec.parseSuite(suiteWith(
      """{"id": "r1", "dimension": "validity", "severity": "error", "row_level": true,
         "check": {"kind": "range", "column": "a", "min": 0, "max": 10, "include_max": false}},
        {"id": "r2", "dimension": "validity", "severity": "warn",
         "check": {"kind": "in_set", "column": "s", "values": ["X", "Y"]}},
        {"id": "r3", "dimension": "accuracy", "severity": "warn",
         "check": {"kind": "mean", "column": "a", "assert": {"op": "between", "min": 1, "max": 2}}}"""))
    suite.rules.map(_.check) shouldBe Seq(
      Range("a", 0, 10, includeMin = true, includeMax = false),
      InSet("s", Seq("X", "Y")),
      Mean("a", Assertion("between", None, Some(Bound.Literal(1)), Some(Bound.Literal(2)))))
  }

  private def rejects(json: String, expected: String): Unit = {
    val e = intercept[SuiteFormatException](RuleSpec.parseSuite(json))
    e.getMessage should include(expected)
  }

  test("từ chối dimension ngoài 6 dimension") {
    rejects(suiteWith("""{"id": "r", "dimension": "distribution", "severity": "warn",
      "check": {"kind": "complete", "column": "a"}}"""), "dimension='distribution'")
  }

  test("từ chối rule id trùng") {
    val r = """{"id": "r", "dimension": "validity", "severity": "warn", "check": {"kind": "complete", "column": "a"}}"""
    rejects(suiteWith(s"$r, $r"), "trùng")
  }

  test("từ chối row_level cho kind không có kết quả theo dòng") {
    rejects(suiteWith("""{"id": "r", "dimension": "accuracy", "severity": "warn", "row_level": true,
      "check": {"kind": "mean", "column": "a", "assert": {"op": ">=", "value": 1}}}"""), "không hỗ trợ row_level")
  }

  test("từ chối assertion thiếu giá trị, op lạ, regex sai, format sai") {
    rejects(suiteWith("""{"id": "r", "dimension": "completeness", "severity": "error",
      "check": {"kind": "size", "assert": {"op": ">="}}}"""), "cần 'value'")
    rejects(suiteWith("""{"id": "r", "dimension": "completeness", "severity": "error",
      "check": {"kind": "size", "assert": {"op": "~", "value": 1}}}"""), "op='~'")
    rejects(suiteWith("""{"id": "r", "dimension": "validity", "severity": "error",
      "check": {"kind": "pattern", "column": "a", "regex": "(abc"}}"""), "regex không hợp lệ")
    rejects("""{"format": "gx", "rules": []}""", "chỉ hỗ trợ 'dq-rules/v2'")
  }
}
