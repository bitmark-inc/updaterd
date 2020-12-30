# updaterd

## Installation

To compile use use the `git` command to clone the repository and the
`go` command to compile all commands.  The process requires that the
Go installation be 1.15 or later as the build process uses Go Modules.

~~~~~
git clone https://github.com/bitmark-inc/updaterd
cd updaterd
go install
~~~~~

## Configuration

Create the configuration directory, copy sample configuration, edit it
to set up blockchain connections and the database.

~~~~~
mkdir -p ~/.config/updaterd
cp updaterd.conf.sample  ~/.config/updaterd/updaterd.conf
${EDITOR} ~/.config/updaterd/updaterd.conf
~~~~~

Generate the the key pair for communicating with Bitmark blockchain nodes.

~~~~~
updaterd --config-file=~/.config/updaterd/updaterd.conf generate-identity
~~~~~

## Setup and run updaterd

Create database tables and functions if you run updaterd for the first time by using the following command:

~~~~~
sh share/install-schema --create --using=~/.config/updaterd/updaterd.conf --pg-host={PG_ADMIN_HOST} --pg-pass={PG_ADMIN_PASS}
~~~~~

Start the program.

~~~~~
updaterd --config-file="${HOME}/.config/updaterd/updaterd.conf"
~~~~~
