# install_pyodbc.sh content
#!/bin/sh
export DYLD_LIBRARY_PATH=/opt/homebrew/lib:$DYLD_LIBRARY_PATH
export ODBCINSTINI=/opt/homebrew/etc/odbcinst.ini
export ODBCINI=/opt/homebrew/etc/odbc.ini
pip install --no-binary :all: pyodbc

# Make script executable and run it
