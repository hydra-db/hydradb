#!/usr/bin/env python3
"""Fetch or verify the exact locked upstream corpus; never resolve a ref."""
import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
LOCK = ROOT / "query-language/cypher/tck/upstream.lock.json"
REPOSITORY = "https://github.com/opencypher/openCypher.git"
FIELDS = {"schema_version", "repository", "commit", "corpus_subdirectory", "digest_algorithm",
          "corpus_sha256", "feature_files", "scenario_definitions", "corpus_bytes", "license",
          "license_path", "source"}


def git(directory, *args):
    return subprocess.check_output(["git", "-C", str(directory), *args], text=True).strip()


def load_lock():
    lock = json.loads(LOCK.read_text(encoding="utf-8"))
    if (set(lock) != FIELDS or lock["schema_version"] != 1
        or lock["repository"] != REPOSITORY or lock["corpus_subdirectory"] != "tck/features"
        or lock["digest_algorithm"] != "sha256-length-prefixed-path-and-content-v1"
        or not re.fullmatch(r"[0-9a-f]{40}", lock["commit"])
        or not re.fullmatch(r"[0-9a-f]{64}", lock["corpus_sha256"])
        or any(type(lock[k]) is not int or lock[k] <= 0 for k in
               ("feature_files", "scenario_definitions", "corpus_bytes"))
        or lock["license"] != "Apache-2.0" or lock["license_path"] != "LICENSE"
        or lock["source"] != f"https://github.com/opencypher/openCypher/tree/{lock['commit']}/tck/features"):
        raise ValueError("invalid lock or moving ref")
    return lock


def verify(directory, lock):
    if git(directory, "rev-parse", "--show-toplevel") != str(directory):
        raise ValueError("destination must be the repository root")
    if git(directory, "rev-parse", "HEAD") != lock["commit"]:
        raise ValueError("wrong HEAD")
    if subprocess.run(["git", "-C", str(directory), "symbolic-ref", "-q", "HEAD"],
                      stdout=subprocess.DEVNULL).returncode == 0:
        raise ValueError("HEAD must be detached, not a moving ref")
    corpus = directory / lock["corpus_subdirectory"]
    if not corpus.is_dir() or not (directory / lock["license_path"]).is_file():
        raise ValueError("missing corpus or license")
    files = sorted(corpus.rglob("*"))
    if any(p.is_symlink() or (not p.is_file() and not p.is_dir()) for p in files):
        raise ValueError("corpus contains symlink or special file")
    files = [p for p in files if p.is_file()]
    if any(p.suffix != ".feature" for p in files):
        raise ValueError("unexpected non-feature corpus file")
    digest = hashlib.sha256()
    scenarios = size = 0
    for path in files:
        name = path.relative_to(corpus).as_posix().encode("utf-8")
        data = path.read_bytes()
        digest.update(len(name).to_bytes(8, "big")); digest.update(name)
        digest.update(len(data).to_bytes(8, "big")); digest.update(data)
        size += len(data)
        scenarios += len(re.findall(rb"^\s*(?:Scenario|Scenario Outline):", data, re.MULTILINE))
    if (len(files), scenarios, size, digest.hexdigest()) != (lock["feature_files"],
            lock["scenario_definitions"], lock["corpus_bytes"], lock["corpus_sha256"]):
        raise ValueError("corpus inventory or digest mismatch")
    print(f"Verified {lock['commit']}: {len(files)} files, {scenarios} definitions, sha256 {digest.hexdigest()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("fetch", "check"))
    parser.add_argument("destination", type=Path, help="checkout outside the workspace")
    args = parser.parse_args()
    directory = args.destination.resolve()
    if directory == ROOT or ROOT in directory.parents:
        raise ValueError("destination must be outside the workspace")
    lock = load_lock()
    if args.action == "fetch":
        if directory.exists():
            raise ValueError("fetch destination already exists; use check or choose a new path")
        directory.mkdir(parents=True)
        git(directory, "init", "--quiet")
        git(directory, "remote", "add", "origin", lock["repository"])
        git(directory, "fetch", "--quiet", "--depth=1", "origin", lock["commit"])
        git(directory, "checkout", "--quiet", "--detach", "FETCH_HEAD")
    verify(directory, lock)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError, subprocess.CalledProcessError) as error:
        print(f"openCypher corpus verification failed: {error}", file=sys.stderr)
        sys.exit(1)
