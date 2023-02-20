# WTT01

## Build Details

| GitHub Job    | Purpose                                             |
| ------------- | --------------------------------------------------- |
| DevDocker.yml | Build and push dev tags to AWS dev account          |
| PrdDocker.yml | Same as DevDocker.yml but for PRD                   |
| Linux.yml     | Builds the extensions in several linux environments |
| MacOS.yml     | Builds a universal extension on MacOS               |
| Windows.yml   | Builds a windows extension                          |

### Linux

Linux currently builds for manylinux which builds the wheels with GCC4 making them compatible with Python. Even though this extension can be used in Python on Linux, it can not be used from the command line.
