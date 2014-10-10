#!/usr/bin/env python
import code

# interactive console for debugging
def debug_console(**kwargs):
    code.interact("debug console",
                local=kwargs)

