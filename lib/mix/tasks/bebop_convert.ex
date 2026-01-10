# SPDX-License-Identifier: AGPL-3.0-or-later

defmodule Mix.Tasks.Sysobs.BebopConvert do
  use Mix.Task

  @shortdoc "Convert NDJSON events to Bebop frames (observability channel)"

  @moduledoc """
  Usage:

      mix sysobs.bebop_convert --input /path/to/events.jsonl --outdir /path/to/out
  """

  alias SystemObservatory.BebopConverter

  @impl true
  def run(args) do
    {opts, _rest, _} =
      OptionParser.parse(args,
        switches: [
          input: :string,
          outdir: :string
        ]
      )

    input = opts[:input] || raise "Missing --input"
    outdir = opts[:outdir] || raise "Missing --outdir"

    BebopConverter.convert_file(input, outdir)
    :ok
  end
end
