#!/bin/bash
uvicorn app:app --host 0.0.0.0 --port 7080 >& logs/server-$(date '+%d.%m.%Y-%H.%M').log
