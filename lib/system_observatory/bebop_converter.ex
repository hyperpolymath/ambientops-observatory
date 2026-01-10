# SPDX-License-Identifier: AGPL-3.0-or-later

defmodule SystemObservatory.BebopConverter do
  @moduledoc """
  Convert AmbientOps JSON/NDJSON events into Bebop frames for observability.
  """

  require Logger

  @schema_path Path.expand("../../bebop/schemas/ambientops_events.bop", __DIR__)

  @type_map %{
    "placement_decision" => "PlacementDecision",
    "log_scan" => "LogScan",
    "state_vault_capture" => "StateVaultCapture",
    "unmanaged_detection" => "UnmanagedDetection",
    "unmanaged_suggestion" => "UnmanagedSuggestion"
  }

  def convert_file(input_path, out_dir) do
    events = read_events(input_path)
    File.mkdir_p(out_dir)

    Enum.map(events, fn event ->
      case convert_event(event, out_dir) do
        {:ok, path} -> path
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  def convert_event(event, out_dir) do
    event_type = Map.get(event, "event_type")
    type = Map.get(@type_map, event_type)

    if type do
      payload = payload_for(event_type, event)
      encode_bebop(event, type, payload, out_dir)
    else
      {:error, :unsupported_event}
    end
  end

  defp read_events(path) do
    case File.read(path) do
      {:ok, content} ->
        content
        |> String.split("\n", trim: true)
        |> Enum.flat_map(&decode_line/1)

      _ ->
        []
    end
  end

  defp decode_line(line) do
    case Jason.decode(line) do
      {:ok, event} -> [event]
      _ -> []
    end
  end

  defp payload_for("placement_decision", event) do
    payload = Map.get(event, "payload", %{})

    %{
      operation_id: Map.get(payload, "operation_id", ""),
      package_id: Map.get(payload, "package_id", ""),
      intent: Map.get(payload, "intent", ""),
      profile: Map.get(payload, "profile", ""),
      selected_surface: Map.get(payload, "selected_surface", ""),
      result: Map.get(payload, "result", ""),
      dry_run: Map.get(payload, "dry_run", false)
    }
  end

  defp payload_for("log_scan", event) do
    payload = Map.get(event, "payload", %{})
    findings = Map.get(payload, "findings", [])

    %{
      findings: Enum.map(findings, &finding_payload/1),
      since: Map.get(payload, "since", ""),
      limit: Map.get(payload, "limit", 0)
    }
  end

  defp payload_for("state_vault_capture", event) do
    payload = Map.get(event, "payload", %{})

    %{
      operation_id: Map.get(payload, "operation_id", ""),
      package_id: Map.get(payload, "package_id", ""),
      vault_path: Map.get(payload, "vault_path", ""),
      entry_dir: Map.get(payload, "entry_dir", ""),
      dry_run: Map.get(payload, "dry_run", false)
    }
  end

  defp payload_for("unmanaged_detection", event) do
    payload = Map.get(event, "payload", %{})
    entries = Map.get(payload, "entries", [])
    %{entries: Enum.map(entries, &entry_payload/1)}
  end

  defp payload_for("unmanaged_suggestion", event) do
    payload = Map.get(event, "payload", %{})
    entries = Map.get(payload, "entries", [])
    %{entries: Enum.map(entries, &entry_payload/1)}
  end

  defp payload_for(_event_type, _event), do: %{}

  defp finding_payload(finding) do
    %{
      source: Map.get(finding, "source", ""),
      category: Map.get(finding, "category", ""),
      line: Map.get(finding, "line", "")
    }
  end

  defp entry_payload(entry) do
    %{
      path: Map.get(entry, "path", ""),
      name: Map.get(entry, "name", ""),
      kind: Map.get(entry, "kind", ""),
      origin: Map.get(entry, "origin", ""),
      suggested_surface: Map.get(entry, "suggested_surface", ""),
      suggested_route: Map.get(entry, "suggested_route", ""),
      origin_confidence: Map.get(entry, "origin_confidence", "") |> to_string()
    }
  end

  defp encode_bebop(event, type, payload, out_dir) do
    if System.find_executable("bebopc") do
      tmp_path = temp_json_path()
      File.write!(tmp_path, Jason.encode!(payload))

      args = ["encode", "--schema", @schema_path, "--type", type, "--json", tmp_path]
      {bin, status} = System.cmd("bebopc", args, stderr_to_stdout: true)

      if status == 0 do
        output_path = output_path(out_dir, event, "bebop")
        File.write!(output_path, bin)
        {:ok, output_path}
      else
        {:error, :bebopc_failed}
      end
    else
      Logger.warning("bebopc not found; writing JSON payload instead")
      output_path = output_path(out_dir, event, "json")
      File.write!(output_path, Jason.encode!(payload))
      {:ok, output_path}
    end
  end

  defp output_path(out_dir, event, ext) do
    event_type = Map.get(event, "event_type", "event")
    timestamp = Map.get(event, "timestamp", "unknown") |> String.replace(":", "-")
    Path.join(out_dir, "#{event_type}_#{timestamp}.#{ext}")
  end

  defp temp_json_path do
    Path.join(System.tmp_dir!(), "bebop_payload_#{System.unique_integer([:positive])}.json")
  end
end
