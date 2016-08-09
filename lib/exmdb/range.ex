defmodule Exmdb.Range do
  defstruct from: :first, to: :last, direction: :fwd, src: nil, db_spec: nil, close_txn?: true

  def new(env_or_txn, opts \\ []) do
    {_dbi, key_type, _val_type} = db_spec = get_db_spec(env_or_txn, opts)
    from = opts |> Keyword.get(:from, :first) |> validate_range(key_type)
    to = opts |> Keyword.get(:to, :last) |> validate_range(key_type)
    direction = direction(from, to)
    close_txn = case env_or_txn do
                  %Exmdb.Env{} -> true
                  %Exmdb.Txn{} -> false
                end
    %Exmdb.Range{
      src: env_or_txn,
      from: from,
      to: to,
      direction: direction,
      db_spec: db_spec,
      close_txn?: close_txn
    }
  end

  defp get_db_spec(%Exmdb.Env{dbs: dbs}, opts) do
    Exmdb.expand_db_spec(dbs, opts)
  end
  defp get_db_spec(%Exmdb.Txn{env: env}, opts) do
    Exmdb.expand_db_spec(env.dbs, opts)
  end

  defp validate_range({:key, key}, key_type) do
    Exmdb.encode(key, key_type)
  end
  defp validate_range(:first, _key_type) do
    :first
  end
  defp validate_range(:last, _key_type) do
    :last
  end
  defp validate_range(badarg, _key_type) do
    raise ArgumentError, message: "expected :first, :last, or {:key, key}, got: #{inspect badarg}"
  end

  defp direction(:first, _to), do: :fwd
  defp direction(:last, _to), do: :bwd
  defp direction(_from, :last), do: :fwd
  defp direction(_from, :first), do: :bwd
  defp direction(from, to) when from <= to, do: :fwd
  defp direction(_from, _to), do: :bwd
end

defimpl Enumerable, for: Exmdb.Range do
  alias Exmdb.{Env, Range, Txn}

  def count(_range) do
    { :error, __MODULE__ }
  end

  def member?(%Range{db_spec: {dbi, key_type, val_type}}, {key, val}) do
    {:ok, case :elmdb.get(dbi, Exmdb.encode(key, key_type)) do
            {:ok, bin} ->
              Exmdb.decode(bin, val_type) == val
            :not_found ->
              false
            {:error, {_code, msg}} ->
              raise List.to_string(msg)
          end}
  end

  def reduce(%Range{} = range, acc, fun) do
    with {:ok, range} <- ensure_txn(range),
         {:ok, cur} <- cursor_open(range) do
      case start(range, cur, acc, fun) do
        {:cont, acc} ->
          close(range, cur)
          {:done, acc}
        {:suspend, acc} ->
          {:suspended, acc}
        {:halt, acc} ->
          close(range, cur)
          {:halted, acc}
        {:error, {_code, msg}} ->
          raise List.to_string(msg)
      end
    else
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end

  defp start(range, cur, {:cont, acc}, fun) do
    {_dbi, key_type, val_type} = range.db_spec
    with {init_op, cont_op, limit} <- prepare(range, cur),
         {:ok, key, val} <- cursor_get(cur, init_op, range.src.type) do
      acc = fun.({Exmdb.decode(key, key_type), Exmdb.decode(val, val_type)}, acc)
      reduce_cursor(range, {cur, cont_op, limit}, acc, fun)
    else
      :not_found ->
        {:cont, acc}
      error ->
        error
    end
  end

  defp reduce_cursor(range, cur_spec, {:cont, acc}, fun) do
    case apply(range, cur_spec, acc, fun) do
      {:ok, acc} ->
        reduce_cursor(range, cur_spec, acc, fun)
      done ->
        done
    end
  end
  defp reduce_cursor(_range, _cur_spec, { :halt, acc }, _fun) do
    { :halt, acc }
  end
  defp reduce_cursor(range, cur_spec, { :suspend, acc }, fun) do
    { :suspend, acc, &reduce_cursor(range, cur_spec, &1, fun) }
  end

  defp apply(%Range{db_spec: db_spec, src: %Txn{type: txn_type}}, {cur, cont_op, limit}, acc, fun) do
    {_dbi, key_type, val_type} = db_spec
    case cursor_get(cur, cont_op, txn_type) do
      {:ok, key, val} ->
        if binkey_in_range?(key, cont_op, limit) do
          {:ok, fun.({Exmdb.decode(key, key_type), Exmdb.decode(val, val_type)}, acc)}
        else
          {:cont, acc}
        end
      :not_found ->
        {:cont, acc}
      error ->
        error
    end
  end

  defp prepare(range, cur) do
    limit = range.to
    cont_op = get_cont_op(range.direction)
    case get_init_op(range, cur) do
      {:error, e} ->
        {:error, e}
      init_op ->
        {init_op, cont_op, limit}
    end
  end

  defp get_cont_op(:fwd), do: :next
  defp get_cont_op(:bwd), do: :prev

  defp get_init_op(%Range{from: :first}, _cur), do: :first
  defp get_init_op(%Range{from: :last}, _cur), do: :last
  defp get_init_op(%Range{from: key, direction: :fwd}, _cur), do: {:set_range, key}
  defp get_init_op(%Range{from: key, direction: :bwd, src: %Txn{type: txn_type}}, cur) do
    case cursor_get(cur, {:set_range, key}, txn_type) do
      {:ok, ^key, _val} ->
        {:set, key}
      {:ok, _key, _val} ->
        case cursor_get(cur, :prev, txn_type) do
          {:ok, new_key, _val} ->
            {:set, new_key}
          other ->
            other
        end
      :not_found ->
        :last
      error ->
        error
    end
  end

  defp ensure_txn(%Range{src: %Env{res: env_res} = env} = range) do
    case :elmdb.ro_txn_begin(env_res) do
      {:ok, txn_res} ->
        txn = %Txn{res: txn_res, env: env, type: :ro}
        {:ok, %Range{range|src: txn}}
      error ->
        error
    end
  end
  defp ensure_txn(%Range{src: %Txn{}} = range) do
    {:ok, range}
  end

  defp cursor_open(%Range{src: %Txn{res: txn_res, type: txn_type}, db_spec: db_spec}) do
    {dbi, _key_type, _val_type} = db_spec
    case txn_type do
      :ro ->
        :elmdb.ro_txn_cursor_open(txn_res, dbi)
      :rw ->
        :elmdb.txn_cursor_open(txn_res, dbi)
    end
  end

  defp cursor_get(cur, op, :ro) do
    :elmdb.ro_txn_cursor_get(cur, op)
  end
  defp cursor_get(cur, op, :rw) do
    :elmdb.txn_cursor_get(cur, op)
  end

  defp binkey_in_range?(binkey, :next, to) do
    to == :last or binkey <= to
  end
  defp binkey_in_range?(binkey, :prev, to) do
    to == :first or binkey >= to
  end

  defp close(%Range{close_txn?: false, src: %Txn{type: :ro}}, cur) do
    :elmdb.ro_txn_cursor_close(cur)
  end
  defp close(%Range{close_txn?: false, src: %Txn{type: :rw}}, _cur) do
    :ok
  end
  defp close(%Range{close_txn?: true, src: %Txn{res: res, type: :ro}}, cur) do
    :elmdb.ro_txn_abort(res)
    :elmdb.ro_txn_cursor_close(cur)
  end
  defp close(%Range{close_txn?: true, src: %Txn{res: res, type: :rw}}, _cur) do
    :elmdb.txn_commit(res)
  end
end
