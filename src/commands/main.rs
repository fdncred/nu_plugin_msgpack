use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{Category, LabeledError, Signature, Value};

use crate::MsgPackPlugin;

pub struct Main;

impl SimplePluginCommand for Main {
    type Plugin = MsgPackPlugin;

    fn name(&self) -> &str {
        "msgpack"
    }

    fn usage(&self) -> &str {
        "MsgPack commands for Nushell."
    }

    fn extra_usage(&self) -> &str {
        ""
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name()).category(Category::Formats)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["msgpack", "plugins"]
    }

    fn run(
        &self,
        _plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        Ok(Value::string(engine.get_help()?, call.head))
    }
}
