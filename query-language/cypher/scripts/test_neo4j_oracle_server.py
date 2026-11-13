"""Exercise container ownership safeguards without starting or deleting Docker containers."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class OracleOwnershipTests(unittest.TestCase):
    def invoke(self, *, owned=False, running=True, image_ok=True, exists=True, command="up"):
        with tempfile.TemporaryDirectory(prefix="oracle-ownership-") as temporary:
            root = Path(temporary)
            state = str(root / "state")
            server = Path(__file__).with_name("neo4j_oracle_server.sh")
            image = "neo4j:test@sha256:" + "a" * 64
            inventory = {"exists": exists, "running": running, "owner": "cypher-compat-oracle-v1" if owned else "someone-else",
                         "state": state, "image": image if image_ok else "neo4j:another-image"}
            (root / "inventory.json").write_text(json.dumps(inventory))
            docker = root / "docker"
            docker.write_text("""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
p = Path(os.environ['FAKE_ROOT'])
d = json.loads((p / 'inventory.json').read_text())
args = sys.argv[1:]
with (p / 'calls.jsonl').open('a') as f: f.write(json.dumps(args) + '\\n')
if args[0] == 'inspect':
    if not d['exists']: sys.exit(1)
    fmt = args[2]
    if 'State.Running' in fmt: print(str(d['running']).lower())
    elif 'Config.Image' in fmt: print(d['image'])
    elif 'neo4j-oracle-state' in fmt: print(d['state'])
    elif 'neo4j-oracle' in fmt: print(d['owner'])
    else: print('fake-id')
elif args[0] == 'logs': print('Started.')
elif args[0] == 'run': print('new-id')
""")
            docker.chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"], FAKE_ROOT=str(root),
                       NEO4J_ORACLE_STATE=state, NEO4J_ORACLE_IMAGE=image, NEO4J_ORACLE_CONTAINER="test-oracle")
            result = subprocess.run(["bash", str(server), command], env=env, capture_output=True, text=True)
            calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()]
            return result, calls

    def test_unowned_running_container_is_not_reused(self):
        result, calls = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unowned", result.stderr)
        self.assertFalse(any(c[0] in {"rm", "run"} for c in calls))

    def test_unowned_stopped_container_is_not_removed(self):
        result, calls = self.invoke(running=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(c[0] == "rm" for c in calls))

    def test_owned_running_container_with_wrong_image_is_preserved(self):
        result, calls = self.invoke(owned=True, image_ok=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("different image", result.stderr)
        self.assertFalse(any(c[0] == "rm" for c in calls))

    def test_owned_matching_container_is_reused(self):
        result, calls = self.invoke(owned=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(c[0] in {"rm", "run"} for c in calls))

    def test_down_preserves_foreign_container(self):
        result, calls = self.invoke(command="down")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(c[0] == "rm" for c in calls))

    def test_down_removes_owned_container(self):
        result, calls = self.invoke(command="down", owned=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any(c[:3] == ["rm", "-f", "-v"] for c in calls))

    def test_new_container_has_ownership_labels(self):
        result, calls = self.invoke(exists=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        run = next(c for c in calls if c[0] == "run")
        self.assertIn("io.hydradb.neo4j-oracle=cypher-compat-oracle-v1", run)
        self.assertTrue(any(v.startswith("io.hydradb.neo4j-oracle-state=") for v in run))


if __name__ == "__main__":
    unittest.main()
