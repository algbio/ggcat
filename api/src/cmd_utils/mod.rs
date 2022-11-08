mod cmd_rewrite;

use crate::cmd_utils::cmd_rewrite::{cmd_rewrite, CmdRewriteArgs};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub enum CmdUtilsArgs {
    Rewrite(CmdRewriteArgs),
}

pub fn process_cmdutils(args: CmdUtilsArgs) {
    match args {
        CmdUtilsArgs::Rewrite(args) => {
            cmd_rewrite(args);
        }
    }
}
