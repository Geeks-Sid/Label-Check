#!/bin/sh
set -eu
umask 077

mkdir -p \
    "$INSTANCE_DIR" \
    "$BACKUP_DIR" \
    "$TQ_HOME_DIR" \
    "$(dirname "$SDL_FILE_PATH")"

command_name="${1:-web}"
if [ "$#" -gt 0 ]; then
    shift
fi

case "$command_name" in
    web)
        python -m flask --app app.py validate-security
        python -m flask --app app.py init-db
        exec python -m waitress --host=0.0.0.0 --port="$PORT" app:app
        ;;
    pipeline)
        exec python -u /app/src/pipeline.py "$@"
        ;;
    nightly)
        exec python -u /app/nightly_label_check.py "$@"
        ;;
    stats-scheduler)
        exec python -u /app/src/stats_scheduler.py "$@"
        ;;
    python)
        exec python "$@"
        ;;
    shell)
        exec /bin/sh "$@"
        ;;
    *)
        echo "Unknown container command '$command_name'. Use web, pipeline, nightly, stats-scheduler, python, or shell." >&2
        exit 2
        ;;
esac
