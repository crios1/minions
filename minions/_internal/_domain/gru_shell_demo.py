import cmd
import argparse
import shlex


class GruShell(cmd.Cmd):
    prompt = "gru> "

    def __init__(self):
        super().__init__()

        self.parsers = {}

        self.parsers["minion"] = self._minion_parser()
        self.parsers["pipeline"] = self._pipeline_parser()
        self.parsers["status"] = self._status_parser()
        self.parsers["wait"] = self._wait_parser()
        self.parsers["snapshot"] = self._snapshot_parser()
        self.parsers["fingerprint"] = self._fingerprint_parser()
        self.parsers["deps"] = self._deps_parser()
        self.parsers["redeploy"] = self._redeploy_parser()

        self._register_subcommand_parsers("minion")
        self._register_subcommand_parsers("pipeline")

        self.command_groups = {
            "CORE API": ["minion", "pipeline", "status", "wait"],
            "DEVOPS DESIGN": ["snapshot", "fingerprint", "deps", "redeploy"],
        }

    # ---------- parser builders ----------

    def _minion_parser(self):
        p = argparse.ArgumentParser(
            prog="minion",
            description="Start or stop a minion.",
            add_help=False,
        )
        sub = p.add_subparsers(dest="action", required=True)
        p._subparsers = sub  # for help registration

        start = sub.add_parser(
            "start",
            description="Start a minion.",
            add_help=False,
        )
        a = start.add_argument
        a("m_modpath")
        a("m_configpath")
        a("p_modpath")

        stop = sub.add_parser(
            "stop",
            description="Stop a minion.",
            add_help=False,
        )
        stop.add_argument("mid")
        return p

    def _pipeline_parser(self):
        p = argparse.ArgumentParser(
            prog="pipeline",
            description="Pause or resume a pipeline.",
            add_help=False,
        )
        sub = p.add_subparsers(dest="action", required=True)
        p._subparsers = sub

        pause = sub.add_parser(
            "pause",
            description="Pause a pipeline.",
            add_help=False,
        )
        pause.add_argument("pid")

        resume = sub.add_parser(
            "resume",
            description="Resume a pipeline.",
            add_help=False,
        )
        resume.add_argument("pid")
        return p

    def _status_parser(self):
        p = argparse.ArgumentParser(
            prog="status",
            description="Instant snapshot of current state.",
            add_help=False,
        )
        p.add_argument("scope", nargs="?", choices=["work", "pipeline", "minion"])
        p.add_argument("id", nargs="?")
        return p

    def _wait_parser(self):
        p = argparse.ArgumentParser(
            prog="wait",
            description="Wait for IDs to leave transitional states.",
            add_help=False,
        )
        p.add_argument("ids", nargs="+")
        p.add_argument("--timeout", type=int)
        return p

    def _snapshot_parser(self):
        return argparse.ArgumentParser(
            prog="snapshot",
            description="Print canonical sorted JSON snapshot.",
            add_help=False,
        )

    def _fingerprint_parser(self):
        return argparse.ArgumentParser(
            prog="fingerprint",
            description="Hash of the current snapshot.",
            add_help=False,
        )

    def _deps_parser(self):
        p = argparse.ArgumentParser(
            prog="deps",
            description="Show dependencies for a minion, pipeline, or resource.",
            add_help=False,
        )
        p.add_argument("kind", choices=["minion", "pipeline", "resource"])
        p.add_argument("id")
        return p

    def _redeploy_parser(self):
        p = argparse.ArgumentParser(
            prog="redeploy",
            description="Redeploy a minion/pipeline/resource by ID.",
            add_help=False,
        )
        p.add_argument("id")
        p.add_argument("--strategy", choices=["drain", "cutover"], default="drain")
        p.add_argument("--timeout", type=int, required=True)
        return p

    # ---------- helpers ----------

    def _register_subcommand_parsers(self, name):
        parser = self.parsers.get(name)
        sub = getattr(parser, "_subparsers", None)
        if not sub:
            return
        for sub_name, sub_parser in sub.choices.items():
            self.parsers[f"{name} {sub_name}"] = sub_parser

    def _parse(self, name, line):
        parser = self.parsers[name]
        try:
            return parser.parse_args(shlex.split(line or ""))
        except SystemExit:
            return None

    def _print_parser_help(self, topic, parser):
        print(parser.format_help(), end="")
        if topic not in ("minion", "pipeline"):
            return
        prefix = f"{topic} "
        subcommands = sorted(
            t for t in self.parsers.keys()
            if t.startswith(prefix) and t != topic
        )
        if not subcommands:
            return
        print("\nSubcommands:")
        for sub in subcommands:
            desc = self.parsers[sub].description or ""
            print(f"  {sub:<20} {desc}")

    # ---------- core API commands ----------

    def do_minion(self, line):
        args = self._parse("minion", line)
        if not args:
            return
        if args.action == "start":
            self._minion_start(args)
            return
        if args.action == "stop":
            self._minion_stop(args)
            return

    def _minion_start(self, args):
        print(
            f"[MOCK] minion start m_modpath={args.m_modpath} "
            f"m_configpath={args.m_configpath} p_modpath={args.p_modpath}"
        )
        print("[MOCK] wid=wid-123")

    def _minion_stop(self, args):
        print(f"[MOCK] minion stop mid={args.mid}")
        print("[MOCK] wid=wid-456")

    def help_minion(self):
        self._print_parser_help("minion", self.parsers["minion"])

    def do_pipeline(self, line):
        args = self._parse("pipeline", line)
        if not args:
            return
        if args.action == "pause":
            self._pipeline_pause(args)
            return
        if args.action == "resume":
            self._pipeline_resume(args)
            return

    def _pipeline_pause(self, args):
        print(f"[MOCK] pipeline pause pid={args.pid}")
        print("[MOCK] wid=wid-789")

    def _pipeline_resume(self, args):
        print(f"[MOCK] pipeline resume pid={args.pid}")
        print("[MOCK] wid=wid-987")

    def help_pipeline(self):
        self._print_parser_help("pipeline", self.parsers["pipeline"])

    def do_status(self, line):
        args = self._parse("status", line)
        if not args:
            return
        if not args.scope and not args.id:
            print("[MOCK] status: global snapshot (work, minions, pipelines)")
            return
        if args.scope and not args.id:
            print(f"[MOCK] status: scope={args.scope}")
            return
        if args.scope and args.id:
            print(f"[MOCK] status: scope={args.scope} id={args.id}")
            return
        print(f"[MOCK] status: id={args.id}")

    def help_status(self):
        self._print_parser_help("status", self.parsers["status"])

    def do_wait(self, line):
        args = self._parse("wait", line)
        if not args:
            return
        try:
            print(f"[MOCK] waiting for ids={args.ids} timeout={args.timeout}")
        except KeyboardInterrupt:
            print("\n[INFO] wait cancelled by user; jobs continue running")
            return

    def help_wait(self):
        self._print_parser_help("wait", self.parsers["wait"])

    # ---------- devops design commands ----------

    def do_snapshot(self, line):
        args = self._parse("snapshot", line)
        if not args:
            return
        print('[MOCK] {"work": [], "minions": [], "pipelines": []}')

    def help_snapshot(self):
        self._print_parser_help("snapshot", self.parsers["snapshot"])

    def do_fingerprint(self, line):
        args = self._parse("fingerprint", line)
        if not args:
            return
        print("[MOCK] fingerprint=deadbeef1234")

    def help_fingerprint(self):
        self._print_parser_help("fingerprint", self.parsers["fingerprint"])

    def do_deps(self, line):
        args = self._parse("deps", line)
        if not args:
            return
        print(f"[MOCK] deps kind={args.kind} id={args.id}")

    def help_deps(self):
        self._print_parser_help("deps", self.parsers["deps"])

    def do_redeploy(self, line):
        args = self._parse("redeploy", line)
        if not args:
            return
        print(
            f"[MOCK] redeploy id={args.id} "
            f"strategy={args.strategy} timeout={args.timeout}"
        )

    def help_redeploy(self):
        self._print_parser_help("redeploy", self.parsers["redeploy"])

    # ---------- top-level help & exit ----------

    def do_help(self, arg):
        name = arg.strip()
        if name:
            parser = self.parsers.get(name)
            if parser:
                self._print_parser_help(name, parser)
                return
            func = getattr(self, f"help_{name}", None)
            if func:
                func()
                return
            print(f"No help for {name}")
            return

        print("Documented commands (type help <topic>):")
        print("========================================\n")

        print("Shell:")
        print("  exit         Exit the shell")
        print("  quit         Exit the shell")
        print("  EOF          Exit the shell (Ctrl+D or Ctrl+Z)\n")

        for header, cmds in self.command_groups.items():
            print(f"{header}:")
            for cmd_name in cmds:
                parser = self.parsers.get(cmd_name)
                if not parser:
                    continue
                desc = parser.description or ""
                print(f"  {cmd_name:<12} {desc}")
            print()

    def do_exit(self, line):
        return True

    def do_quit(self, line):
        return True

    def do_EOF(self, line):
        print()
        return True

if __name__ == "__main__":
    shell = GruShell()
    while True:
        try:
            shell.cmdloop()
            break
        except KeyboardInterrupt:
            print()
            continue
