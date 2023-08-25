import argparse
import re
import subprocess
from multiprocessing.pool import ThreadPool


def parse_args():
    parser = argparse.ArgumentParser(
        description='Calculate the commit list from the branch that should be applied to upstream for rebasing'
    )

    parser.add_argument('-b', '--branch', help='The branch', required=True)
    parser.add_argument('-u', '--upstream', help='Upstream', required=True)
    return parser.parse_args()


def get_commit_symmetric_difference(not_in: str, has_in: str) -> list[str]:
    result = subprocess.run(
        ["git", "rev-list", "--cherry-pick", "--right-only", "--no-merges", f"{not_in}..{has_in}"],
        stdout=subprocess.PIPE,
        check=True
    )
    return result.stdout.decode().split()


class CommitInfo:
    def __init__(self, author: str, date: str, message_first_line: str, patch_hash: str):
        self.author = author
        self.date = date
        self.message_first_line = message_first_line
        self.patch_hash = patch_hash


AUTHOR_EMAIL_REGEX = re.compile(b"^.*<(.+@.+)>$")


def get_commit_patch_id(commit: str) -> tuple[str, CommitInfo]:
    git_show_result = subprocess.run(
        ["git", "show", commit],
        stdout=subprocess.PIPE,
        check=True
    )

    lines = map(lambda l: l.strip(), git_show_result.stdout.splitlines())
    lines = [line for line in filter(lambda l: l, lines)]

    author = lines[1]
    author_regex_result = AUTHOR_EMAIL_REGEX.search(author)
    if author_regex_result:
        author = author_regex_result.group(1)
    author = author.decode(errors='replace').replace('\uFFFD', '?')

    date = lines[2].strip().strip(b'Date: ').strip(b' +0000').decode(errors='replace').replace('\uFFFD', '?')
    message_first_line = lines[3].decode(errors='replace').replace('\uFFFD', '?')

    result = subprocess.run(
        ["git", "patch-id"],
        stdout=subprocess.PIPE,
        input=git_show_result.stdout,
        check=True
    )

    patch_id = ""
    raw_out = result.stdout.decode()
    if raw_out:
        patch_id = raw_out.split()[0]
    return commit, CommitInfo(author, date, message_first_line, patch_id)


def build_patch_id_map(commits: list[str]) -> dict[str, CommitInfo]:
    result = {}
    with ThreadPool() as pool:
        done = 0
        for commit, patch_id in pool.imap_unordered(get_commit_patch_id, commits, chunksize=256):
            result[commit] = patch_id
            done += 1
            if done >= len(commits) / 100:
                print(f"Ready: {int(100 * (len(result) / len(commits)))} %", flush=True)
                done = 0
    return result


def inverse_map(m: dict[str, CommitInfo]) -> dict[str, list[str]]:
    result = {}
    for commit, info in m.items():
        if info.patch_hash:
            result.setdefault(info.patch_hash, []).append(commit)

    return result


def main():
    args = parse_args()
    branch = args.branch
    upstream = args.upstream

    has_in_branch_not_in_upstream = get_commit_symmetric_difference(not_in=upstream, has_in=branch)
    has_in_upstream_not_in_branch = get_commit_symmetric_difference(not_in=branch, has_in=upstream)

    print(f"Start calculating branch_patch_id_map({len(has_in_branch_not_in_upstream)})")
    branch_patch_id_map = build_patch_id_map(has_in_branch_not_in_upstream)
    branch_patch_id_inv_map = inverse_map(branch_patch_id_map)

    duplicates = {}
    for commits_with_same_patch in branch_patch_id_inv_map.values():
        if len(commits_with_same_patch) > 1:
            for commit in commits_with_same_patch:
                duplicates[commit] = [c for c in filter(lambda c: c != commit, commits_with_same_patch)]

    print(f"Start calculating upstream_patch_id_map({len(has_in_upstream_not_in_branch)})")
    upstream_patch_id_map = build_patch_id_map(has_in_upstream_not_in_branch)
    upstream_patch_id_inv_map = inverse_map(upstream_patch_id_map)

    empty_commits = 0
    has_in_brunch_but_found_in_upstream = {}
    for commit, commit_info in branch_patch_id_map.items():
        if not commit_info.patch_hash:
            empty_commits += 1
        elif commit_info.patch_hash in upstream_patch_id_inv_map:
            has_in_brunch_but_found_in_upstream[commit] = upstream_patch_id_inv_map[commit_info.patch_hash]

    print(f"total to apply: {len(has_in_branch_not_in_upstream)}")
    print(f"empty commits: {empty_commits}")
    print(f"found in upstream: {len(has_in_brunch_but_found_in_upstream)}")
    effective = len(has_in_branch_not_in_upstream) - len(has_in_brunch_but_found_in_upstream) - empty_commits
    print(f"total to apply without found and empty: {effective}")
    print("--------------------------------------------------\n")
    for commit in reversed(has_in_branch_not_in_upstream):
        extra_msgs = []
        commit_info = branch_patch_id_map[commit]
        if commit in has_in_brunch_but_found_in_upstream:
            extra_msgs.append(f"found in upstream [{', '.join(has_in_brunch_but_found_in_upstream[commit])}]")
        if branch_patch_id_map[commit] is None:
            extra_msgs.append("empty commit")
        if commit in duplicates:
            extra_msgs.append(f"duplicate of [{', '.join(duplicates[commit])}]")
        extra_msg_str = "; ".join(extra_msgs)
        if extra_msg_str:
            extra_msg_str = f"\t{extra_msg_str}"
        print(f"{commit}\t{commit_info.date}\t{commit_info.author}\t{commit_info.message_first_line}{extra_msg_str}")


if __name__ == '__main__':
    main()

