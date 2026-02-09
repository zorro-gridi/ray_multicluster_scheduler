#!/bin/bash
#
# Knowledge Capture Hook for debugger
#
# This script is triggered when the debugger subagent completes.
# It prompts the user to save the analysis as a knowledge base entry.
#

set -e

# Project directory
PROJECT_DIR="${CLAUDE_PROJECT_DIR:-.}"
KNOWLEDGE_BASE_DIR="$PROJECT_DIR/.knowledge-base"
TEMPLATE_FILE="$KNOWLEDGE_BASE_DIR/templates/case-template.md"

# Check if template exists
if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "⚠️  Warning: Template file not found at $TEMPLATE_FILE"
    exit 0
fi

echo ""
echo "📚 Exception Analysis Complete"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Would you like to save this analysis as a knowledge base entry?"
echo ""
echo "Options:"
echo "  1. Yes - create new knowledge base entry"
echo "  2. No - skip"
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
        slug=$(echo "$title" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-\|-$//g')
        filename="${date_str}-${slug}.md"
        target_file="$KNOWLEDGE_BASE_DIR/exceptions/$category/$filename"

        # Copy template
        cp "$TEMPLATE_FILE" "$target_file"

        echo ""
        echo "✅ Knowledge base entry created: $target_file"
        echo ""
        echo "Next steps:"
        echo "  1. Edit the file to fill in the details"
        echo "  2. Update the category index: $KNOWLEDGE_BASE_DIR/exceptions/$category/index.md"
        echo ""
        ;;
    2)
        echo "Skipping knowledge base entry creation."
        ;;
    *)
        echo "Invalid choice. Skipping."
        ;;
esac

exit 0
