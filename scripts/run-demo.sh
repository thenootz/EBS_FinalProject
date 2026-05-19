#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────────
#  EBS Pub/Sub — Demo runner
#  Builds and runs the system end-to-end with optional flags
# ────────────────────────────────────────────────────────────────────────────

set -e

# ── Paths ──────────────────────────────────────────────────────────────────
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

JAR="target/pubsub-1.0.jar"
CLASSPATH="$JAR"

# ── Colors ─────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# ── Helpers ────────────────────────────────────────────────────────────────
log() { echo -e "${GREEN}[demo]${NC} $1"; }
warn() { echo -e "${YELLOW}[demo]${NC} $1"; }
err() { echo -e "${RED}[demo]${NC} $1"; }

# ── Clean ports first ──────────────────────────────────────────────────────
log "Clearing previously used ports..."
fuser -k 5001/tcp 5002/tcp 5003/tcp 7001/tcp 7002/tcp 7003/tcp 2>/dev/null || true
sleep 1

# ── Build (Maven shaded jar) if missing ────────────────────────────────────
if [ ! -f "$JAR" ]; then
    log "Building project with Maven..."
    if ! command -v mvn >/dev/null 2>&1; then
        err "mvn not found. Install with: sudo apt install maven"
        exit 1
    fi
    mvn -q -o clean package -DskipTests || mvn -q clean package -DskipTests
fi

if [ ! -f "$JAR" ]; then
    err "Build failed — $JAR not produced"
    exit 1
fi

# ── Parse mode argument ────────────────────────────────────────────────────
MODE="${1:-demo}"

case "$MODE" in
    demo)
        log "Running 30-second demo (300 subs, plain mode)"
        java -cp "$CLASSPATH" ebs.Main
        ;;
    encrypted)
        log "Running 30-second demo with AES-GCM encryption"
        java -cp "$CLASSPATH" ebs.Main --encrypted
        ;;
    fault)
        log "Running 40-second demo with broker failure at t=10s"
        java -cp "$CLASSPATH" ebs.Main --fault-test
        ;;
    eval)
        FEED_SECONDS="${2:-180}"
        log "Running full evaluation: 10k subs × 2 scenarios × ${FEED_SECONDS}s"
        warn "This will take approximately $((FEED_SECONDS * 2 / 60 + 2)) minutes..."
        java -Dfeed.seconds="$FEED_SECONDS" -cp "$CLASSPATH" ebs.EvalHarness
        log "Results saved to eval-results.csv"
        ;;
    test)
        log "Running unit tests via Maven..."
        mvn -q test
        ;;
    clean)
        log "Cleaning build artifacts..."
        mvn -q clean
        rm -f eval-results.csv
        log "Done"
        ;;
    help|*)
        echo "Usage: $0 <mode> [options]"
        echo ""
        echo "Modes:"
        echo "  demo                  Run 30s demo (default)"
        echo "  encrypted             Run 30s demo with encryption (bonus)"
        echo "  fault                 Run 40s demo with broker failure (bonus)"
        echo "  eval [seconds]        Run full evaluation (default 180s)"
        echo "  test                  Compile and run all unit tests"
        echo "  clean                 Remove build artifacts"
        echo "  help                  Show this help"
        echo ""
        echo "Examples:"
        echo "  $0 demo"
        echo "  $0 eval 60            # Quick 60-second evaluation"
        echo "  $0 fault"
        ;;
esac
