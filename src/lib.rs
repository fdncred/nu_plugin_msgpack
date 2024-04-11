use nu_plugin::{Plugin, PluginCommand};

mod commands;
mod msgpack;

pub use commands::*;
pub use msgpack::MsgPackPlugin;

impl Plugin for MsgPackPlugin {
    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        // This is a list of all of the commands you would like Nu to register when your plugin is
        // loaded.
        //
        // If it doesn't appear on this list, it won't be added.
        vec![Box::new(Main), Box::new(FromMsgpack), Box::new(ToMsgpack)]
    }
}
