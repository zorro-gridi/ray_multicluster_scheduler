#!/bin/bash
#
# Verified Knowledge Capture Hook for Stop Event
#
# This script is triggered when the main conversation completes (Stop event).
# It intelligently detects if exception analysis occurred and whether solutions were verified.
#

set -e

# Project directory
PROJECT_DIR="${CLAUDE_PROJECT_DIR:-.}"
KNOWLEDGE_BASE_DIR="$PROJECT_DIR/.knowledge-base"
TEMPLATE_FILE="$KNOWLEDGE_BASE_DIR/templates/case-template.md"

# Check if template exists
if [ ! -f "$TEMPLATE_FILE" ]; then
    exit 0
fi

# Read transcript path from stdin
INPUT=$(cat -)
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')

if [ -z "$TRANSCRIPT_PATH" ] || [ ! -f "$TRANSCRIPT_PATH" ]; then
    exit 0
fi

# Check if exception-analyzer was used in this session
if ! grep -q "exception-analyzer" "$TRANSCRIPT_PATH" 2>/dev/null; then
    exit 0
fi

# Auto-detect test results from transcript
echo ""
echo "📚 Exception Analysis Detected"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Detection variables
HAS_PASSED=false
HAS_FAILED=false
TEST_OUTPUT=""

# Check for test success markers
if grep -qiE "passed|OK|PASSED|success" "$TRANSCRIPT_PATH" 2>/dev/null; then
    HAS_PASSED=true
    TEST_OUTPUT="✅ Detected test success markers"
fi

# Check for test failure markers
if grep -qiE "failed|ERROR|FAILED|assertion" "$TRANSCRIPT_PATH" 2>/dev/null; then
    HAS_FAILED=true
    TEST_OUTPUT="❌ Detected test failure markers"
fi

# Show detection results
if [ "$HAS_PASSED" = true ] && [ "$HAS_FAILED" = false ]; then
    echo "$TEST_OUTPUT"
    echo ""
    echo "💡 Suggestion: This exception appears to be verified and resolved."
    echo "   Recommended status: **verified**"
elif [ "$HAS_FAILED" = true ]; then
    echo "$TEST_OUTPUT"
    echo ""
    echo "⚠️  Warning: Tests may still be failing."
    echo "   Recommended status: **draft** (for later verification)"
else
    echo "ℹ️  No explicit test results detected in this session."
    echo ""
    echo "💡 You can still save this case if you've verified it manually."
fi

echo ""
echo "Would you like to save this analysis as a knowledge base entry?"
echo ""
echo "Options:"
echo "  1. Yes - save as verified (solution tested and working)"
echo "  2. Yes - save as draft (needs further verification)"
echo "  3. No - skip"
echo ""
read -p "Your choice (1-3): " choice

case $choice in
    1|2)
        # Determine verification status
        if [ "$choice" = "1" ]; then
            verified="true"
            verified_date=$(date +%Y-%m-%d)
        else
            verified="false"
            verified_date=""
        fi

        echo ""
        echo "Enter the following information for the knowledge base entry:"
        echo ""

        # Get exception category
        echo "Available categories:"
        echo "  1. cluster-connection"
        echo "  2. resource-scheduling"
        echo "  3. concurrency"
        echo "  4. configuration"
        echo "  5. policy-engine"
        echo "  6. circuit-breaker"
        echo ""
        read -p "Select category (1-6): " cat_choice

        case $cat_choice in
            1) category="cluster-connection" ;;
            2) category="resource-scheduling" ;;
            3) category="concurrency" ;;
            4) category="configuration" ;;
            5) category="policy-engine" ;;
            6) category="circuit-breaker" ;;
            *) category="cluster-connection" ;;
        esac

        # Get exception title
        read -p "Exception title (short description): " title

        # Generate filename
        date_str=$(date +%Y-%m-%d)
        slug=$(echo "$title" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-\|-$//g')
        filename="${date_str}-${slug}.md"
        target_file="$KNOWLEDGE_BASE_DIR/exceptions/$category/$filename"

        # Read template
        template_content=$(cat "$TEMPLATE_FILE")

        # Replace placeholders with actual values
        case_content=$(echo "$template_content" | sed "s/created: YYYY-MM-DD/created: $date_str/")
        case_content=$(echo "$case_content" | sed "s/status: \[draft\/resolved\/verified\]/status: verified/")

        # Add verified fields if verified
        if [ "$verified" = "true" ]; then
            # Insert verified fields after created date
            case_content=$(echo "$case_content" | sed "/created: $date_str/a\\verified: true\\nverified_date: $verified_date")
        fi

        # Write the case file
        echo "$case_content" > "$target_file"

        echo ""
        echo "✅ Knowledge base entry created: $target_file"
        echo ""
        if [ "$verified" = "true" ]; then
            echo "Status: **VERIFIED** - This solution has been tested and confirmed working."
        else
            echo "Status: **DRAFT** - Remember to update this case after verification."
        fi
        echo ""
        echo "Next steps:"
        echo "  1. Edit the file to fill in the details (exception info, root cause, solution)"
        echo "  2. Update the category index: $KNOWLEDGE_BASE_DIR/exceptions/$category/index.md"
        echo ""
        ;;
    3)
        echo "Skipping knowledge base entry creation."
        ;;
    *)
        echo "Invalid choice. Skipping."
        ;;
esac

exit 0
