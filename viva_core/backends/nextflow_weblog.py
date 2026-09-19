"""Nextflow weblog receiver for capturing workflow events.

This module provides the weblog receiver script that runs alongside Nextflow
to capture workflow events via the --web-log flag. Events are written to an
NDJSON file for real-time monitoring and post-hoc analysis.
"""

# Weblog receiver script - runs as a local HTTP server to capture Nextflow events
# This script is embedded in sbatch templates and executed via heredoc
WEBLOG_RECEIVER_SCRIPT = """import json
import os
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler

EVENTS_FILE = os.environ.get('EVENTS_FILE', 'events.ndjson')

class WeblogHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        data = self.rfile.read(length)
        try:
            event = json.loads(data.decode())
            with open(EVENTS_FILE, 'a') as f:
                f.write(json.dumps(event) + chr(10))
        except Exception as ex:
            print("Error processing event:", ex)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('localhost', 0))
port = sock.getsockname()[1]
sock.close()

with open('/tmp/weblog_port_' + str(os.getppid()), 'w') as f:
    f.write(str(port))

print("Weblog receiver starting on port", port, "writing to", EVENTS_FILE)
HTTPServer(('localhost', port), WeblogHandler).serve_forever()
"""


def get_weblog_receiver_bash_block(events_file_path: str) -> str:
    """Generate bash script block for starting the weblog receiver.

    Args:
        events_file_path: Path where the NDJSON events file will be written

    Returns:
        Bash script block that starts the weblog receiver and sets WEBLOG_PORT
    """
    return f'''
### Start weblog receiver for Nextflow event capture
export EVENTS_FILE="{events_file_path}"

python3 << 'WEBLOG_SCRIPT' &
{WEBLOG_RECEIVER_SCRIPT}WEBLOG_SCRIPT

WEBLOG_PID=$!
sleep 1

# Read the port from temp file
WEBLOG_PORT=$(cat /tmp/weblog_port_$$ 2>/dev/null || echo "9999")
rm -f /tmp/weblog_port_$$
echo "Weblog receiver running on port $WEBLOG_PORT (PID: $WEBLOG_PID)"
'''


def get_weblog_cleanup_bash_block() -> str:
    """Generate bash script block for cleaning up the weblog receiver.

    Returns:
        Bash script block that kills the weblog receiver process
    """
    return """
### Cleanup weblog receiver
NF_EXIT_CODE=$?
kill $WEBLOG_PID 2>/dev/null || true
wait $WEBLOG_PID 2>/dev/null || true
"""
