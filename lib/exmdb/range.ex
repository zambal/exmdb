defmodule Exmdb.Range do
  import Exmdb.Util

  defstruct from: :first, to: :last, direction: :fwd, src: nil, db_spec: nil, is_src_owner: false

  def new(env_or_txn, opts \\ []) do
    {_dbi, key_type, _val_type} = db_spec = get_db_spec(env_or_txn, opts)
    from = opts |> Keyword.get(:from, :first) |> validate_range(key_type)
    to = opts |> Keyword.get(:to, :last) |> validate_range(key_type)
    direction = direction(from, to)
    %Exmdb.Range{
      src: env_or_txn,
      from: from,
      to: to,
      direction: direction,
      db_spec: db_spec
    }
  end

  defp get_db_spec(%Exmdb.Env{dbs: dbs}, opts), do: db_spec(dbs, opts)
  defp get_db_spec(%Exmdb.Txn{env: env}, opts), do: db_spec(env.dbs, opts)

  defp validate_range({:key, key}, key_type), do: encode(key, key_type)
  defp validate_range(:first, _key_type), do: :first
  defp validate_range(:last, _key_type), do: :last
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
  import Exmdb.Util

  def count(_range), do: { :error, __MODULE__ }

  def member?(%Range{db_spec: {dbi, key_type, val_type}}, {key, val}) do
    {:ok, case :elmdb.get(dbi, encode(key, key_type)) do
            {:ok, bin}  ->  val == decode(bin, val_type)
            :not_found  -> false
            {:error, e} -> mdb_error(e)
          end}
  end

  def reduce(%Range{} = range, acc, fun) do
    with {:ok, range} <- ensure_txn(range),
         {:ok, cur} <- cursor_open(range) do
      case reduce(range, cur, acc, fun) do
        {:cont, acc} ->
          close(range, cur)
          {:done, acc}
        {:suspend, acc} ->
          {:suspended, acc}
        {:halt, acc} ->
          close(range, cur)
          {:halted, acc}
        {:error, e} ->
          mdb_error(e)
      end
    else
      {:error, e} ->
        mdb_error(e)
    end
  end

  defp reduce(range, cur, {:cont, acc}, fun) do
    {_dbi, key_type, val_type} = range.db_spec
    with {init_op, cont_op, limit} <- prepare(range, cur),
         {:ok, key, val} <- cursor_get(cur, init_op, range.src.type) do
      acc = fun.({decode(key, key_type), decode(val, val_type)}, acc)
      do_reduce({cur, cont_op, limit, key_type, val_type, range.src.type}, acc, fun)
    else
      :not_found ->
        {:cont, acc}
      error ->
        error
    end
  end

  defp do_reduce({cur, cont_op, limit, key_type, val_type, txn_type} = state, {:cont, acc}, fun) do
    case cursor_get(cur, cont_op, txn_type) do
      {:ok, key, val} ->
        if binkey_in_range?(key, cont_op, limit) do
          do_reduce(state, fun.({decode(key, key_type), decode(val, val_type)}, acc), fun)
        else
          {:cont, acc}
        end
      :not_found ->
        {:cont, acc}
      error ->
        error
    end
  end
  defp do_reduce(_state, { :halt, acc }, _fun) do
    { :halt, acc }
  end
  defp do_reduce(state, { :suspend, acc }, fun) do
    { :suspend, acc, &do_reduce(state, &1, fun) }
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
        {:ok, %Range{range|src: txn, is_src_owner: true}}
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
      :ro -> :elmdb.ro_txn_cursor_open(txn_res, dbi)
      :rw -> :elmdb.txn_cursor_open(txn_res, dbi)
    end
  end

  defp cursor_get(cur, op, :ro), do: :elmdb.ro_txn_cursor_get(cur, op)
  defp cursor_get(cur, op, :rw), do: :elmdb.txn_cursor_get(cur, op)

  defp binkey_in_range?(binkey, :next, to), do: to == :last or binkey <= to
  defp binkey_in_range?(binkey, :prev, to), do: to == :first or binkey >= to

  defp close(%Range{is_src_owner: true, src: %Txn{res: res, type: :ro}}, cur) do
    :elmdb.ro_txn_abort(res)
    :elmdb.ro_txn_cursor_close(cur)
  end
  defp close(%Range{is_src_owner: true, src: %Txn{res: res, type: :rw}}, _cur) do
    :elmdb.txn_commit(res)
  end
  defp close(%Range{is_src_owner: false, src: %Txn{type: :ro}}, cur) do
    :elmdb.ro_txn_cursor_close(cur)
  end
  defp close(%Range{is_src_owner: false, src: %Txn{type: :rw}}, _cur) do
    :ok
  end
end
