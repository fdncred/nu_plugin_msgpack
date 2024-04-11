use crate::MsgPackPlugin;
use chrono::DateTime;
use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{record, Category, Example, LabeledError, Record, Signature, Span, Type, Value};
use rmpv::decode::read_value_ref;

pub struct FromMsgpack;

impl SimplePluginCommand for FromMsgpack {
    type Plugin = MsgPackPlugin;

    fn name(&self) -> &str {
        "from msgpack"
    }

    fn usage(&self) -> &str {
        "Convert from msgpack to structured data."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .category(Category::Formats)
            .switch("brotli", "Decompress brotli encoded binary data", Some('b'))
            .input_output_type(Type::Binary, Type::Any)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["example", "configuration"]
    }

    fn examples(&self) -> Vec<Example> {
        vec![
            Example {
                example: "0x[81A86772656574696E67AB68656C6C6F20776F726C64] | from msgpack",
                description: "Decode msgpack",
                result: Some(Value::test_record(record! {
                    "greeting" => Value::test_string("hello world")
                })),
            },
            Example {
                example: "0x[81A86772656574696E67C41D1F2000F88D54B5BF64737B2B90B31CA411563A0358CEB1891C642AEE72] | from msgpack --brotli",
                description: "Decode msgpack with a brotli encoded string",
                result: Some(Value::test_record(record! {
                    "greeting" => Value::test_string("hello world this is a long string")
                })),
            },
            Example {
                example: "open helloworld.msgpack",
                description: "Load msgpack from a file",
                result: None,
            }
        ]
    }

    fn run(
        &self,
        _plugin: &MsgPackPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        let decompress = call.has_flag("brotli")?;
        let mut bin = input.as_binary()?;

        let v = match read_value_ref(&mut bin) {
            Err(e) => {
                return Err(
                    LabeledError::new(e.to_string()).with_label("Invalid msgpack", Span::unknown())
                );
            }
            Ok(v) => v,
        };

        rmpv_to_nu(v, decompress)
    }
}

/// Convert [rmpv::Value] to a [nu_protocol::Value].
pub fn rmpv_to_nu(value: rmpv::ValueRef<'_>, decompress: bool) -> Result<Value, LabeledError> {
    let span = Span::unknown();
    Ok(match value {
        rmpv::ValueRef::Nil => Value::nothing(span),
        rmpv::ValueRef::Boolean(b) => Value::bool(b, span),
        rmpv::ValueRef::Integer(i) => {
            let i = i.as_i64().ok_or(
                LabeledError::new(
                    "Encountered a msgpack integer bigger than what nushell supports (i64::MAX).",
                )
                .with_label("Integer overflow", span),
            )?;
            Value::int(i, span)
        }
        rmpv::ValueRef::F32(f) => Value::float(f.into(), span),
        rmpv::ValueRef::F64(f) => Value::float(f, span),
        rmpv::ValueRef::String(s) => {
            let s = s.into_str().ok_or(
                LabeledError::new("Encountered a msgpack string that was not valid UTF-8")
                    .with_label("Invalid UTF-8", span),
            )?;
            Value::string(s, span)
        }
        rmpv::ValueRef::Binary(b) => {
            if decompress {
                let mut decompressed = Vec::<u8>::new();
                match brotli::BrotliDecompress(&mut b.as_ref(), &mut decompressed) {
                    Ok(_) => Value::string(String::from_utf8_lossy(&decompressed), span),
                    Err(_) => Value::binary(b, span),
                }
            } else {
                Value::binary(b, span)
            }
        }
        rmpv::ValueRef::Array(vs) => {
            let vs: Result<_, LabeledError> =
                vs.into_iter().map(|v| rmpv_to_nu(v, decompress)).collect();
            Value::list(vs?, span)
        }
        rmpv::ValueRef::Map(map) => {
            let mut record = Record::new();

            for (k, v) in map {
                record.insert(
                    rmpv_to_nu(k, decompress)?.coerce_string()?,
                    rmpv_to_nu(v, decompress)?,
                );
            }

            Value::record(record, span)
        }
        rmpv::ValueRef::Ext(discriminant, data) => {
            match discriminant {
                // timestamp extension type
                -1 => ext_timestamp_to_nu(data)?,
                _ => unknown_ext_to_nu(discriminant, data),
            }
        }
    })
}

/// Convert a msgpack ext value with an unrecognized type to a nu record.
fn unknown_ext_to_nu(discriminant: i8, data: &[u8]) -> Value {
    let record = [
        ("ext_type", Value::int(discriminant.into(), Span::unknown())),
        ("data", Value::binary(data, Span::unknown())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    Value::record(record, Span::unknown())
}

/// Convert a msgpack timestamp ext type (-1) to a nu date value.
/// See [https://github.com/msgpack/msgpack/blob/master/spec.md].
fn ext_timestamp_to_nu(data: &[u8]) -> Result<Value, LabeledError> {
    let seconds: i64;
    let nanos: u32;
    match data.len() {
        4 => {
            let data = <&[u8; 4]>::try_from(data).expect("slice has correct len");
            nanos = 0;
            seconds = u32::from_be_bytes(*data).into();
        }
        8 => {
            let data = <&[u8; 8]>::try_from(data).expect("slice has correct len");
            let data = u64::from_be_bytes(*data);

            // seconds are stored as 34 bits, nanos as 30.
            nanos = (data >> 34) as u32;
            seconds = (data & 0x00000003ffffffff) as i64;
        }
        12 => {
            let data_nsec = <&[u8; 4]>::try_from(&data[..4]).expect("slice has correct len");
            let data_sec = <&[u8; 8]>::try_from(&data[4..12]).expect("slice has correct len");
            nanos = u32::from_be_bytes(*data_nsec);
            seconds = i64::from_be_bytes(*data_sec);
        }
        n => {
            return Err(LabeledError::new(format!(
                "Parsed ext type -1 (timestamp) with invalid length {n}"
            ))
            .with_label("Invalid timestamp length", Span::unknown()));
        }
    }

    let date = DateTime::from_timestamp(seconds, nanos).ok_or_else(|| {
        LabeledError::new(format!(
            "Timestamp value (seconds={}, nanos={}) is out of range",
            seconds, nanos
        ))
        .with_label("Timestamp out of range", Span::unknown())
    })?;

    Ok(Value::date(date.into(), Span::unknown()))
}

#[test]
fn test_examples() -> Result<(), nu_protocol::ShellError> {
    use nu_plugin_test_support::PluginTest;
    PluginTest::new("msgpack", MsgPackPlugin.into())?.test_command_examples(&FromMsgpack)
}
