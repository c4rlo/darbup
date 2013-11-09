# darbup
## Regular backups using _dar_

_darbup_ is a wrapper around the excellent [_dar_](http://dar.linux.free.fr/) archiver tool.

### Features

- **Schedule regular full and incremental backups to any mounted filesystem (e.g. external HDD)**
- **Useful pre-canned policies for removal of old backups**
- Fully configurable
- Everything is logged

### Requirements

- _dar_
- _Python_ 3.3 or better
- _Linux_

Finally, note that _darbup_ is designed for backup to a mounted filesystem, not to online backup services. Having said that, if you can mount your online storage via something like _sshfs_, you should be able to use that.

### Usage

```
$ git clone https://github.com/c4rlo/darbup.git && cd darbup
$ ./darbup
Default configuration file written to /home/carlo/.darbup/config, please customize
$ your_favourite_editor ~/.darbup/config
```

The generated config file has explanatory comments next to all the options.

Next, we install _darbup_ as a cronjob, to ensure it gets executed regularly. If your system has [_anacron_](http://anacron.sourceforge.net/), you could use that; but be careful to ensure you run `darbup` as the desired user (i.e. probably not _root_).

However, **when _darbup_ is run, it first checks whether it needs to do anything, according to the configured schedule; if not, it just exits.** Hence, there is no harm in running it more frequently than needed. Myself, I just set it to run every hour.

```
$ { crontab -l; echo '30 * * * * ionice -c 3 nice -17 /home/carlo/darbup/darbup'; } | crontab -
```

Some things to note here:

- `crontab` is the program used to install per-user cron schedules (aka crontabs)
  - `crontab -l` lists the current crontab
  - `crontab -` installs a new crontab from stdout, replacing the existing one
- `nice -17` (part of `coreutils`) lowers the CPU scheduling priority for `darbup`
- `ionice -c 3` (part of `util-linux`) sets the I/O scheduling class for `darbup` to class 3, which is _Idle_

I use `nice` and `ionice` to ensure my system remains responsive during backups.

### Author and license

_darbup_ was created by Carlo Teubner. My email address is my first name, then a dot, then my last name, at gmail dot com.

_darbup_ is Copyright 2013 Carlo Teubner. It is licensed under the GPL 3, which is reproduced in the accompanying file named `COPYING`.
