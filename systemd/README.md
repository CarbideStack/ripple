# Ripple systemd Units

This directory contains systemd service units for running Ripple and emitting
an event when the system starts.

## Units

- `ripple.service` runs the Ripple event-dispatch daemon. It starts
  `/usr/bin/ripple --daemon --gid ripple`, supports reloads through
  `systemctl reload ripple`, and restarts automatically after failures.
- `ripple-startup.service` is a one-shot unit that emits the `startup` event
  after `ripple.service` is ready. Handlers registered for `startup` can use it
  to perform work during system startup.

The main service asks systemd to create the directories Ripple needs:

- `/run/ripple` for the daemon's Unix socket.
- `/etc/ripple/handlers` for executable event handlers.
- `/var/lib/ripple` for persistent timer state.

Members of the dedicated `ripple` group are allowed to connect to the daemon.
Add each user that should manage Ripple to this group.

Install the unit files in `/etc/systemd/system/`, reload systemd, and enable the
units that should run at boot:

```sh
sudo groupadd --system ripple
sudo cp ripple.service ripple-startup.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ripple.service
sudo systemctl enable ripple-startup.service
```

If the group already exists, `groupadd` reports that fact and no action is
needed. To grant an existing user access, add the user to the group and start a
new login session:

```sh
sudo usermod --append --groups ripple USER
```

Enabling `ripple-startup.service` schedules the `startup` event for subsequent
boots. Start it manually if the event should also be emitted immediately:

```sh
sudo systemctl start ripple-startup.service
```
