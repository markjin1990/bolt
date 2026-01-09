/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstdint>
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;

class CheckedArithmeticTest : public functions::test::FunctionBaseTest {
  template <typename T>
  std::optional<T> checkedAdd(
      const std::optional<T> a,
      const std::optional<T> b) {
    return evaluateOnce<T>("checked_add(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> checkedDivide(
      const std::optional<T> a,
      const std::optional<T> b) {
    return evaluateOnce<T>("checked_divide(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> checkedMultiply(
      const std::optional<T> a,
      const std::optional<T> b) {
    return evaluateOnce<T>("checked_multiply(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> checkedSubtract(
      const std::optional<T> a,
      const std::optional<T> b) {
    return evaluateOnce<T>("checked_subtract(c0, c1)", a, b);
  }

  template <typename T>
  void assertErrorForCheckedArithmetic(
      const std::string& func,
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    auto res = evaluateOnce<T>(fmt::format("try({}(c0, c1))", func), a, b);
    ASSERT_TRUE(!res.has_value());
    try {
      evaluateOnce<T>(fmt::format("{}(c0, c1)", func), a, b);
      FAIL() << "Expected an error";
    } catch (const std::exception& e) {
      ASSERT_TRUE(
          std::string(e.what()).find(errorMessage) != std::string::npos);
    }
  }

  template <typename T>
  void assertErrorForCheckedAdd(
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    assertErrorForCheckedArithmetic("checked_add", a, b, errorMessage);
  }

  template <typename T>
  void assertErrorForCheckedDivide(
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    assertErrorForCheckedArithmetic("checked_divide", a, b, errorMessage);
  }

  template <typename T>
  void assertErrorForCheckedMultiply(
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    assertErrorForCheckedArithmetic("checked_multiply", a, b, errorMessage);
  }

  template <typename T>
  void assertErrorForcheckedSubtract(
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    assertErrorForCheckedArithmetic("checked_subtract", a, b, errorMessage);
  }
};

TEST_F(CheckedArithmeticTest, Mod) {
  auto firstVector = makeFlatVector<int32_t>({10, 20, 30});
  auto secondVector = makeFlatVector<int32_t>({0, 1, 2});
  auto expected = makeNullableFlatVector<int32_t>({std::nullopt, 0, 0});
#ifndef SPARK_COMPATIBLE
  // When any number mod 0, presto's logic throws an exception
  EXPECT_THROW(
      evaluate<SimpleVector<int32_t>>(
          "mod(c0, c1)", makeRowVector({firstVector, secondVector})),
      BoltUserError);
#else
  auto result = evaluate<SimpleVector<int32_t>>(
      "mod(c0, c1)", makeRowVector({firstVector, secondVector}));
  assertEqualVectors(expected, result);
#endif
}

TEST_F(CheckedArithmeticTest, checkedAdd) {
  assertErrorForCheckedAdd<int8_t>(INT8_MAX, 1, "Arithmetic overflow: 127 + 1");
  assertErrorForCheckedAdd<int16_t>(
      INT16_MAX, 1, "Arithmetic overflow: 32767 + 1");
  assertErrorForCheckedAdd<int32_t>(
      INT32_MAX, 1, "Arithmetic overflow: 2147483647 + 1");
  assertErrorForCheckedAdd<int64_t>(
      INT64_MAX, 1, "Arithmetic overflow: 9223372036854775807 + 1");
  EXPECT_EQ(checkedAdd<float>(kInf, 1), kInf);
  EXPECT_EQ(checkedAdd<double>(kInfDouble, 1), kInfDouble);
}

TEST_F(CheckedArithmeticTest, checkedSubtract) {
  assertErrorForcheckedSubtract<int8_t>(
      INT8_MIN, 1, "Arithmetic overflow: -128 - 1");
  assertErrorForcheckedSubtract<int16_t>(
      INT16_MIN, 1, "Arithmetic overflow: -32768 - 1");
  assertErrorForcheckedSubtract<int32_t>(
      INT32_MIN, 1, "Arithmetic overflow: -2147483648 - 1");
  assertErrorForcheckedSubtract<int64_t>(
      INT64_MIN, 1, "Arithmetic overflow: -9223372036854775808 - 1");
  EXPECT_EQ(checkedSubtract<float>(kInf, 1), kInf);
  EXPECT_EQ(checkedSubtract<double>(kInfDouble, 1), kInfDouble);
}

TEST_F(CheckedArithmeticTest, checkedMultiply) {
  assertErrorForCheckedMultiply<int8_t>(
      INT8_MAX, 2, "Arithmetic overflow: 127 * 2");
  assertErrorForCheckedMultiply<int16_t>(
      INT16_MAX, 2, "Arithmetic overflow: 32767 * 2");
  assertErrorForCheckedMultiply<int32_t>(
      INT32_MAX, 2, "Arithmetic overflow: 2147483647 * 2");
  assertErrorForCheckedMultiply<int64_t>(
      INT64_MAX, 2, "Arithmetic overflow: 9223372036854775807 * 2");
  EXPECT_EQ(checkedMultiply<float>(kInf, 1), kInf);
  EXPECT_EQ(checkedMultiply<double>(kInfDouble, 1), kInfDouble);
}

TEST_F(CheckedArithmeticTest, checkedDivide) {
  assertErrorForCheckedDivide<int32_t>(1, 0, "division by zero");
  assertErrorForCheckedDivide<int8_t>(
      INT8_MIN, -1, "Arithmetic overflow: -128 / -1");
  assertErrorForCheckedDivide<int16_t>(
      INT16_MIN, -1, "Arithmetic overflow: -32768 / -1");
  assertErrorForCheckedDivide<int32_t>(
      INT32_MIN, -1, "Arithmetic overflow: -2147483648 / -1");
  assertErrorForCheckedDivide<int64_t>(
      INT64_MIN, -1, "Arithmetic overflow: -9223372036854775808 / -1");
  EXPECT_EQ(checkedDivide<float>(kInf, 1), kInf);
  EXPECT_EQ(checkedDivide<double>(kInfDouble, 1), kInfDouble);
}
