#!/bin/sh

#---------- HELPER FUNCTIONS ----------#

DIR_SRC_PY="./src/py"

# Clone the full git repo contents into current path
clone() {
  DIR_GIT_TMP="_temp-git"
  git clone git@github.com:smissingham/enterprise-data-report "$DIR_GIT_TMP"
  (cd $DIR_GIT_TMP && tar cf - .) | tar xf -
  rm -rf $DIR_GIT_TMP
}

# Initialise the local directory for development
init() {
  mkdir -p "$DIR_SRC_PY"   # Ensure python src path exists
  cd "$DIR_SRC_PY" || exit # Step into python src path

  # Activate the venv on entry using direnv
  echo ". .venv/bin/activate" >".envrc"

  # Create new pyproject if not exists
  if [ ! -f "pyproject.toml" ]; then
    uv init --python python3 --no-readme
  fi

  # Ensure venv exists
  uv sync

  cd - || exit # Create and step into python src path
}

# Install development & build dependencies
install() {
  init

  cd "$DIR_SRC_PY" || exit # Step into python src path

  # Python base development deps
  uv add \
    ipykernel \
    jupyter

  # Python data handling deps
  uv add \
    numpy \
    polars

  # Python file i/o handling deps
  uv add \
    fastexcel \
    xlsxwriter

  # Python exploratory data analysis deps
  uv add \
    pygwalker

  cd - || exit # Create and step into python src path
}

# Resync all dependency installations
sync() {
  init
  install

  cd "$DIR_SRC_PY" || exit # Step into python src path

  uv sync # Sync the uv python packages

  cd - || exit # Create and step into python src path
}

# Purge all development install files (retains files needed for re-init)
# Then, rerun sync to reinitialise from empty dev deps
reset_dev() {

  # Purge general dev-env files & folders
  rm -rf \
    .direnv/

  if [ -d "$DIR_SRC_PY" ]; then
    cd "$DIR_SRC_PY" || exit # Step into python src path

    # Purge python related files & folders
    rm -rf \
      .venv \
      .python-version \
      main.py \
      pyproject.toml \
      uv.lock

    cd - || exit # Create and step into python src path
  fi

  sync
}

# Purges all files related to development
# Leaves behind only unspecified files (like build files)
purge() {
  git ls-files | xargs rm
}

#---------- WHEN SCRIPT IS EXECUTED ----------#
# If a flag is passed, run the corresponding function
if [ -n "$1" ]; then
  ${1#--}
# Otherwise, run standard setup & execution procedure
else
  clone
  sync
fi
