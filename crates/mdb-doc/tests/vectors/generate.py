import json, datetime
import bson
from bson import encode, Regex, Code, DBRef, MinKey, MaxKey, Binary, Timestamp, Decimal128, ObjectId
from bson.json_util import dumps, CANONICAL_JSON_OPTIONS, RELAXED_JSON_OPTIONS

UTC = datetime.timezone.utc
USER_DEFINED = 128

cases = []
def add(label, v):
    cases.append((label, v))

add("int", {"a": 1})
add("neg_int", {"a": -2147483648})
add("int_overflow_to_long", {"a": 2147483648})
add("long_big", {"a": 9007199254740993})
add("long_max", {"a": 9223372036854775807})
add("long_min", {"a": -9223372036854775808})
add("double_frac", {"a": 1.5})
add("double_whole", {"a": 1.0})
add("double_neg", {"a": -0.125})
add("double_nan", {"a": float("nan")})
add("double_inf", {"a": float("inf")})
add("double_ninf", {"a": float("-inf")})
add("string_ascii", {"a": "hello"})
add("string_empty", {"a": ""})
add("string_unicode", {"a": "键值é\U0001F600"})
add("string_ctrl", {"a": "tab\tnl\nquote\"back\\"})
add("bool_true", {"a": True})
add("bool_false", {"a": False})
add("null", {"a": None})
add("datetime", {"a": datetime.datetime(2023, 11, 14, 22, 13, 20, 123000, tzinfo=UTC)})
add("datetime_epoch", {"a": datetime.datetime(1970, 1, 1, tzinfo=UTC)})
add("oid", {"a": ObjectId("0123456789abcdef01234567")})
add("timestamp", {"a": Timestamp(1, 2)})
add("timestamp_big", {"a": Timestamp(4000000000, 4000000000)})
add("regex", {"a": Regex("^ab.*c$", "im")})
add("regex_empty_opts", {"a": Regex("x", "")})
add("code", {"a": Code("function(){ return 1; }")})
add("code_w_scope", {"a": Code("x = y", scope={"y": 3})})
add("minkey", {"a": MinKey()})
add("maxkey", {"a": MaxKey()})
add("decimal", {"a": Decimal128("1.5")})
add("binary_generic", {"a": Binary(b"\x00\x01\xfe", 0)})
add("binary_md5", {"a": Binary(b"0123456789abcdef", 5)})
add("binary_userdefined", {"a": Binary(b"xx", USER_DEFINED)})
add("binary_uuid", {"a": Binary(b"0123456789abcdef", 4)})
add("binary_old", {"a": Binary(b"old", 2)})
add("array", {"a": [1, "two", 3.0, True, None]})
add("array_nested", {"a": [[1, 2], {"k": "v"}]})
add("array_empty", {"a": []})
add("doc_nested", {"a": {"b": {"c": {"d": 1}}}})
add("doc_empty", {"a": {}})
add("key_unicode", {"键": "值"})
add("key_dollar", {"$weird": 1})
add("key_dots", {"a.b": 1, "a": {"b": 2}})
add("order_preserved", {"z": 1, "a": 2, "m": 3})
add("deep_20", {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": 1}}}}}}}}}}}}}}}}}}}}})
add("wide_100", {("k%d" % i): i for i in range(100)})
add("dbref", {"a": DBRef("coll", ObjectId("0123456789abcdef01234567"))})

out = []
for label, v in cases:
    out.append({
        "label": label,
        "hex": encode(v).hex(),
        "canonical": dumps(v, json_options=CANONICAL_JSON_OPTIONS),
        "relaxed": dumps(v, json_options=RELAXED_JSON_OPTIONS),
    })

with open("vectors.jsonl", "w", encoding="utf-8", newline="\n") as f:
    for rec in out:
        f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
print("cases:", len(out))
