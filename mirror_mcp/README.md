# Mirror-Market in your own assistant — trial setup

You are getting a private connection from the assistant you already use (Claude,
ChatGPT, Cursor or Gemini CLI) to Mirror-Market's daily soy data. You ask
questions in plain words; the assistant pulls the numbers from Mirror-Market's
tools and cites the date and source for each one.

**The URL you need is private to you.** The owner sends it to you separately;
it looks like `https://<app>.fly.dev/mcp/<long-token>`. Do not post it
anywhere. Paste it where each client below says `<URL from owner>`.

## What this is, and is not

- **A daily edition, not a feed.** The data refreshes once a day after the US
  session closes. Every number carries its own date; the assistant will tell
  you how old it is. If you need a tick, this is the wrong tool.
- **Nothing here is a settlement.** Board prices are the provider's delayed
  close; physical prices are assessments or administered prices. The assistant
  is told to say so, never to call a number a settlement, executable or a firm
  offer, and to run its draft through a language check before quoting a price.
- **Prices are USD/MT** unless a row says otherwise. Spreads and crush margins
  are only ever struck on one session's numbers; when a leg is missing the
  answer is "withheld, because …", never a guess.
- **Read-only.** There are no positions, no orders, nothing of yours stored.
  The only thing recorded is *which tool was called when* (never the answer
  text), so the owner can see whether this was useful over two weeks.

## Connect it

### Claude Desktop (free plan is enough; one custom connector allowed)

1. Open Claude Desktop → **Settings** → **Connectors**.
2. **Add custom connector**. Name: `Mirror-Market`. URL: `<URL from owner>`.
   Leave authentication empty — the URL itself is the key.
3. Save. In a new chat, open the tools menu and check Mirror-Market is on.
4. Ask: *"What is CBOT soybeans worth in USD/MT today, and how old is that?"*

Claude connects to the server from Anthropic's cloud, so this works on any
machine once added; there is no config file to edit.

### ChatGPT (Plus or higher)

1. **Settings** → **Connectors** → **Advanced** → turn on **Developer mode**.
2. **Create** a connector: name `Mirror-Market`, MCP server URL `<URL from
   owner>`, authentication **None**.
3. In a new chat, enable the connector from the tools menu and ask the same
   question.

### Cursor

Add to `~/.cursor/mcp.json` (create the file if needed):

```json
{
  "mcpServers": {
    "mirror-market": { "url": "<URL from owner>" }
  }
}
```

Restart Cursor; the tools appear under MCP in the agent panel.

### Gemini CLI

Add to `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "mirror-market": { "httpUrl": "<URL from owner>" }
  }
}
```

Start `gemini` and run `/mcp` to confirm the eleven tools are listed.

## Ten questions to try

1. What is CBOT soybeans worth in USD/MT, and what date is that from?
2. What is the Dalian crush margin on the named month, and is it one session's numbers?
3. Which origin is cheapest landed into North China for the next shipment window, and by how much?
4. What is the Gulf basis over the named CBOT contract today?
5. Who has repriced since the board moved — which markets printed and which did not?
6. Is any data layer over its freshness budget right now?
7. What is the Mississippi stage at St. Louis, and is that observed or forecast?
8. Are there weather alerts in Mato Grosso this week, and what was withheld?
9. Where does the Argentina FOB number come from, and what price type is it?
10. Check this sentence before I send it: "Beans' settlement price was 390 and the Gulf has a firm offer at 410."

The right answer to several of these is "withheld" or "empty" with a reason.
That is the tool working, not failing.

## What the trial measures

The question being tested: **does conversational access to Mirror-Market help a
physical buyer understand the market and answer useful questions faster?** —
tested before any positions or landed-cost workflow.

- Window: 10 trading days after the friend's first successful call.
- PASS requires all of: (a) friend asks ≥1 question on ≥6 of the 10 days (from the question log); (b) owner spot-checks 5 answers the friend screenshots against the site's same-date numbers — 5/5 match and 0 forbidden-claim words; (c) exit interview: friend says it was faster than the site for the majority of his questions and names ≥1 question it answered that the site did not; (d) 0 deploy-step failures that left a stale DB live for >1 day.
- FAIL on any of: <4 usage days; any wrong number; friend stops after week 1.
- Readout owner-only, from the readout script (Ticket 4).

## For the owner: the readout

```bash
flyctl ssh console --app mirror-market-mcp -C 'cat /data/questions.jsonl' > .scratch/questions.jsonl
python scripts/mcp_readout.py .scratch/questions.jsonl
```

`.scratch/` is gitignored. The log holds timestamps, tool names, arguments and
outcomes only.
