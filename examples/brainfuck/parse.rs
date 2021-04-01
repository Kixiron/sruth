use std::slice::Iter;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Command {
    IncPtr,
    DecPtr,
    IncValue,
    DecValue,
    Output,
    Input,
    LoopStart,
    LoopEnd,
}

pub(crate) fn parse(source: &str) -> Vec<Command> {
    let mut commands = Vec::with_capacity(source.chars().count());
    commands.extend(source.chars().filter_map(parse_char));

    commands
}

fn parse_char(character: char) -> Option<Command> {
    match character {
        '>' => Some(Command::IncPtr),
        '<' => Some(Command::DecPtr),
        '+' => Some(Command::IncValue),
        '-' => Some(Command::DecValue),
        '.' => Some(Command::Output),
        ',' => Some(Command::Input),
        '[' => Some(Command::LoopStart),
        ']' => Some(Command::LoopEnd),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BrainfuckAst {
    IncPtr,
    DecPtr,
    IncValue,
    DecValue,
    Output,
    Input,
    Loop { body: Vec<BrainfuckAst> },
}

pub(crate) fn stratify(program: &[Command]) -> Vec<BrainfuckAst> {
    let mut ast = Vec::with_capacity(program.len());
    let mut program = program.iter();

    while let Some(command) = program.next() {
        ast.push(stratify_command(command, &mut program));
    }

    ast
}

fn stratify_command(command: &Command, program: &mut Iter<Command>) -> BrainfuckAst {
    match command {
        Command::IncPtr => BrainfuckAst::IncPtr,
        Command::DecPtr => BrainfuckAst::DecPtr,
        Command::IncValue => BrainfuckAst::IncValue,
        Command::DecValue => BrainfuckAst::DecValue,
        Command::Output => BrainfuckAst::Output,
        Command::Input => BrainfuckAst::Input,
        Command::LoopStart => {
            let mut body = Vec::with_capacity(10);

            let mut next = program.next().expect("unexpected EOF");
            while !matches!(next, Command::LoopEnd) {
                body.push(stratify_command(next, program));

                next = program.next().expect("unexpected EOF");
            }

            BrainfuckAst::Loop { body }
        }
        Command::LoopEnd => panic!("unmatched loop end"),
    }
}
