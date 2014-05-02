#!/bin/bash

REPO_ROOT=$( cd $(dirname $(dirname "${BASH_SOURCE[0]}" )) && pwd )

# Source utility functions
source "$REPO_ROOT/tools/utils.sh"

# Check that the necessary environment variables are set.
check_env_variable "DART_SDK"

export PATH="$PATH:$DART_SDK/bin"
export RETURN_VALUE=0


start_phase "Analyzing"
for testfile in $(find $REPO_ROOT/lib -name "*.dart"); do
  analyze_file "$testfile"
  RETURN_VALUE=$(expr $RETURN_VALUE + $?)
done
for testfile in $(find $REPO_ROOT/test -name '*.dart'); do
  analyze_file "$testfile"
  RETURN_VALUE=$(expr $RETURN_VALUE + $?)
done


start_phase "Testing"
for testfile in $(find $REPO_ROOT/test -name "*test.dart"); do
  test_file "$testfile"
  RETURN_VALUE=$(expr $RETURN_VALUE + $?)
done


# Wait until background jobs are done.
wait
echo
echo

exit $RETURN_VALUE
