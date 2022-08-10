# The function checks whether the current branch and
# the supplied one does match. If so, it fetches all new commits and
# merges the local state. If not, it first stashes all local changes,
# switches branches, applies all stashed changes in there and, again,
# fetches all new commits and merges the local state.
function git_handle_branch() {
    if [ "$#" != "1" ]; then
        echo -e "\n * ${COLOR_RED}git_handle_branch expects exactly one argument...${COLOR_RESET}\n"
        exit 1
    fi

    # ----------------------------------------------------------------

    # Check whether the supplied path differs from the current one.
    CURRENT_BRANCH=$(git symbolic-ref HEAD | awk 'BEGIN { FS="/" } { print $3 }')

    if [ "$CURRENT_BRANCH" == "$1" ];then

        # ------------------------------------------------------------

        # Same branch. No switching required.

        # ------------------------------------------------------------

        git fetch || exit 1
        git merge origin/$1 || exit 1

    else

        # ------------------------------------------------------------

        # We need to switch branches.

        # ------------------------------------------------------------

        git fetch || exit 1

        # If there are some local changes, stash them for later uses of
        # the (former) branch.
        if [ "$(git diff-index HEAD | wc -l)" -gt "0" ];then
            git stash || exit 1
        fi

        # ----------------------------------------------------------------

        # Switch to the new branch
        git checkout -f $1 || exit 1

        # ----------------------------------------------------------------

        # If there is a dirty state that got stashed earlier on, reapply
        # it.
        if [ "$(git stash list | grep $1 | wc -l)" -gt "0" ];then
            git stash apply || exit 1
        fi

        # ------------------------------------------------------------

        git fetch || exit 1
        git merge origin/$1 || exit 1

    fi
}
