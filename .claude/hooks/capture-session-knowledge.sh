#!/bin/bash
#
# Session End Knowledge Capture Hook (Backup Mechanism)
#
# This script is triggered when the Claude Code session ends.
# It serves as a backup mechanism to catch any unsaved exception analyses.
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

# Read input from stdin
INPUT=$(cat -)
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')
REASON=$(echo "$INPUT" | jq -r '.reason // empty')

# Exit if no transcript
if [ -z "$TRANSCRIPT_PATH" ] || [ ! -f "$TRANSCRIPT_PATH" ]; then
    exit 0
fi

# Check if debugger was used
if ! grep -q "debugger" "$TRANSCRIPT_PATH" 2>/dev/null; then
    exit 0
fi

# Check if this was a /clear (which means Stop hook should have already triggered)
if [ "$REASON" = "clear" ]; then
    # Stop hook should have handled this, skip to avoid duplicate prompts
    exit 0
fi

echo ""
echo "📚 Session Ending - Exception Analysis Detected"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "This session contained exception analysis that may not have been saved."
echo ""
echo "Options:"
echo "  1. Save now - create knowledge base entry"
echo "  2. Skip - I'll save it later"
echo ""
read -p "Your choice (1-2): " choice

case $choice in
    1)
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
        slug=$(echo "$title" | tr '[:upper:]' '[:lower]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-\|-$//g')
        filename="${date_str}-${slug}.md"
        target_file="$KNOWLEDGE_BASE_DIR/exceptions/$category/$filename"

        # Copy template
        cp "$TEMPLATE_FILE" "$target_file"

        # Update date
        sed -i '' "s/created: YYYY-MM-DD/created: $date_str/" "$target_file"

        echo ""
        echo "✅ Knowledge base entry created: $target_file"
        echo ""
        echo "Status: **DRAFT** - Remember to update this case after verification."
        echo ""
        echo "Next steps:"
        echo "  1. Edit the file to fill in the details"
        echo "  2. Update the status to 'verified' after testing"
        echo "  3. Update the category index: $KNOWLEDGE_BASE_DIR/exceptions/$category/index.md"
        echo ""
        ;;
    2)
        echo "Skipping. You can save this analysis later using the template at:"
        echo "  $TEMPLATE_FILE"
        ;;
    *)
        echo "Invalid choice. Skipping."
        ;;
esac

exit 0
