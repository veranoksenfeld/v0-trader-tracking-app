import { Terminal, Copy, ArrowRight, Wallet, Activity, Users, Settings } from "lucide-react"

function Step({ number, children }: { number: number; children: React.ReactNode }) {
  return (
    <div className="flex gap-4">
      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-indigo-500/20 text-sm font-bold text-indigo-400">
        {number}
      </div>
      <div className="pt-0.5 text-sm leading-relaxed text-zinc-300">{children}</div>
    </div>
  )
}

function FeatureCard({ icon: Icon, title, description }: { icon: React.ElementType; title: string; description: string }) {
  return (
    <div className="rounded-xl border border-zinc-800 bg-zinc-900/60 p-5">
      <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-lg bg-indigo-500/10">
        <Icon className="h-5 w-5 text-indigo-400" />
      </div>
      <h3 className="mb-1 text-sm font-semibold text-white">{title}</h3>
      <p className="text-xs leading-relaxed text-zinc-400">{description}</p>
    </div>
  )
}

function CodeBlock({ children }: { children: string }) {
  return (
    <pre className="overflow-x-auto rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-xs leading-relaxed text-zinc-300">
      <code>{children}</code>
    </pre>
  )
}

export default function Page() {
  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100">
      {/* Hero */}
      <div className="border-b border-zinc-800">
        <div className="mx-auto max-w-4xl px-6 py-20 text-center">
          <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-zinc-800 bg-zinc-900 px-4 py-1.5 text-xs text-zinc-400">
            <Activity className="h-3.5 w-3.5 text-indigo-400" />
            Local Python + Flask Application
          </div>
          <h1 className="mb-4 text-balance text-4xl font-bold tracking-tight text-white md:text-5xl">
            Polymarket Trader Tracker
          </h1>
          <p className="mx-auto max-w-2xl text-pretty text-base leading-relaxed text-zinc-400">
            Track Polymarket traders in real-time, view their profiles and trade history,
            and optionally copy their trades automatically via the CLOB API.
          </p>
        </div>
      </div>

      <div className="mx-auto max-w-4xl px-6 py-16">
        {/* Features Grid */}
        <div className="mb-16 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <FeatureCard
            icon={Users}
            title="Track Traders"
            description="Add any Polymarket wallet address. Profiles and trade history are fetched automatically."
          />
          <FeatureCard
            icon={Activity}
            title="Live Trades"
            description="Fetcher script polls every 60 seconds. Dashboard auto-refreshes every 30 seconds."
          />
          <FeatureCard
            icon={Wallet}
            title="Wallet Balance"
            description="Connect your private key to see your USDC balance and allowance in real time."
          />
          <FeatureCard
            icon={Settings}
            title="Copy Trading"
            description="Mirror trades from selected wallets with configurable size limits and percentages."
          />
        </div>

        {/* Setup Instructions */}
        <div className="mb-16 rounded-2xl border border-zinc-800 bg-zinc-900/40 p-8">
          <div className="mb-8 flex items-center gap-3">
            <Terminal className="h-5 w-5 text-indigo-400" />
            <h2 className="text-lg font-semibold text-white">Setup Instructions</h2>
          </div>

          {/* Prerequisites */}
          <div className="mb-8">
            <h3 className="mb-3 text-sm font-medium text-zinc-300">Prerequisites</h3>
            <p className="text-sm text-zinc-400">
              Install Python 3.8+ from{" "}
              <a href="https://python.org" target="_blank" className="text-indigo-400 underline underline-offset-2 hover:text-indigo-300">
                python.org
              </a>{" "}
              or via <code className="rounded bg-zinc-800 px-1.5 py-0.5 text-xs text-zinc-300">brew install python</code> on macOS.
            </p>
          </div>

          {/* Steps */}
          <div className="space-y-6">
            <Step number={1}>
              <p className="mb-2 font-medium text-white">Download the project</p>
              <p className="mb-3 text-zinc-400">Click the three dots menu in the top-right of this page and select <strong className="text-zinc-200">Download ZIP</strong>, then unzip it. The Flask app lives in the <code className="rounded bg-zinc-800 px-1.5 py-0.5 text-xs text-zinc-300">polymarket-tracker/</code> folder.</p>
            </Step>

            <Step number={2}>
              <p className="mb-2 font-medium text-white">Create a virtual environment and install dependencies</p>
              <CodeBlock>{`cd polymarket-tracker
python3 -m venv venv
source venv/bin/activate    # macOS/Linux
# venv\\Scripts\\activate    # Windows

pip install -r requirements.txt`}</CodeBlock>
            </Step>

            <Step number={3}>
              <p className="mb-2 font-medium text-white">Start the Flask web server (Terminal 1)</p>
              <CodeBlock>{`python app.py`}</CodeBlock>
              <p className="mt-2 text-zinc-400">
                Open{" "}
                <code className="rounded bg-zinc-800 px-1.5 py-0.5 text-xs text-indigo-300">http://localhost:5000</code>{" "}
                in your browser.
              </p>
            </Step>

            <Step number={4}>
              <p className="mb-2 font-medium text-white">Start the trade fetcher (Terminal 2)</p>
              <CodeBlock>{`python fetcher.py`}</CodeBlock>
              <p className="mt-2 text-zinc-400">Polls Polymarket every 60 seconds for new trades from tracked wallets.</p>
            </Step>

            <Step number={5}>
              <p className="mb-2 font-medium text-white">Start the copy trader (Terminal 3 - optional)</p>
              <CodeBlock>{`python copy_trader.py`}</CodeBlock>
              <p className="mt-2 text-zinc-400">
                Only needed if you want to copy trades. Requires your Polymarket private key configured via the Settings panel.
              </p>
            </Step>
          </div>
        </div>

        {/* How to Use */}
        <div className="mb-16 rounded-2xl border border-zinc-800 bg-zinc-900/40 p-8">
          <h2 className="mb-6 text-lg font-semibold text-white">How to Use</h2>
          <div className="space-y-4">
            <div className="flex items-start gap-3">
              <ArrowRight className="mt-0.5 h-4 w-4 shrink-0 text-indigo-400" />
              <p className="text-sm text-zinc-300">
                <strong className="text-white">Add a trader</strong> — Paste any Polymarket wallet address (0x...) into the sidebar input. Find wallet addresses on any Polymarket profile page URL.
              </p>
            </div>
            <div className="flex items-start gap-3">
              <ArrowRight className="mt-0.5 h-4 w-4 shrink-0 text-indigo-400" />
              <p className="text-sm text-zinc-300">
                <strong className="text-white">View trader details</strong> — Click on any trader card to see their profile, stats, and most-traded markets.
              </p>
            </div>
            <div className="flex items-start gap-3">
              <ArrowRight className="mt-0.5 h-4 w-4 shrink-0 text-indigo-400" />
              <p className="text-sm text-zinc-300">
                <strong className="text-white">Setup API key</strong> — Click the red "Setup API Key" button in the header. Enter your private key (from reveal.polymarket.com) and funder address. Your wallet balance will appear once connected.
              </p>
            </div>
            <div className="flex items-start gap-3">
              <ArrowRight className="mt-0.5 h-4 w-4 shrink-0 text-indigo-400" />
              <p className="text-sm text-zinc-300">
                <strong className="text-white">Enable copy trading</strong> — In the Settings modal, enable the master Copy Trading toggle. Then click on individual traders and toggle their Copy Trading switch.
              </p>
            </div>
            <div className="flex items-start gap-3">
              <Copy className="mt-0.5 h-4 w-4 shrink-0 text-indigo-400" />
              <p className="text-sm text-zinc-300">
                <strong className="text-white">Configure trade sizes</strong> — Set copy percentage (e.g., 10% of original), max trade size, and minimum trade size to filter out small trades.
              </p>
            </div>
          </div>
        </div>

        {/* File Structure */}
        <div className="rounded-2xl border border-zinc-800 bg-zinc-900/40 p-8">
          <h2 className="mb-6 text-lg font-semibold text-white">Project Structure</h2>
          <CodeBlock>{`polymarket-tracker/
  app.py              # Flask web server + REST API
  fetcher.py          # Background trade fetcher (runs every 60s)
  copy_trader.py      # Copy trade executor (runs every 30s)
  requirements.txt    # Python dependencies
  config.json         # Auto-created: API credentials + settings
  polymarket_trades.db  # Auto-created: SQLite database
  templates/
    index.html        # Dashboard UI (plain HTML/CSS/JS)`}</CodeBlock>
        </div>

        {/* Footer */}
        <div className="mt-16 text-center text-xs text-zinc-600">
          Download the ZIP and run locally. This preview page is for documentation only.
        </div>
      </div>
    </div>
  )
}
