# Celeriac

A minimal celery worker implementation in clojure.

Celeriac aims to reproduce a majority of the functionality found in the Python
celery worker in a compatiable fasion such that tasks can be dispatched and
harvested normally via celery's Python API, while task authors can benefit from
Clojure's performance and functional paradigm. 

## Usage

`celeriac.core/-main` takes a path to a settings file as its single argument.
The settings file defines the running paramaters for the worker. To see the
demo project in action try running:

```
$ lein run src/demo/settings.json
```

Presently the only settings that will work are using redis as both broker and
results backend, and using json for serialization in both directions. It's
important that your celery app have these same settings, otherwise Celeriac will
be unable to process tasks. Better support of Celery's serialization techniques
is planned, however support for pickeling is pretty far off.


## License

Copyright © 2014 Ryan Jenkins

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
