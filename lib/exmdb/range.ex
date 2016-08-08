defmodule Exmdb.Range do
  defstruct from: :first, to: :last, direction: :fwd, env_res: nil, db_spec: nil, txn: nil

  def new(%Exmdb.Env{} = env, opts \\ []) do
    {_dbi, key_type, _val_type} = db_spec = Exmdb.expand_db_spec(env.dbs, opts)
    from = opts |> Keyword.get(:from, :first) |> validate_range(key_type)
    to = opts |> Keyword.get(:to, :last) |> validate_range(key_type)
    direction = direction(from, to)
    %Exmdb.Range{
      env_res: env.res,
      from: from,
      to: to,
      direction: direction,
      db_spec: db_spec,
      txn: Keyword.get(opts, :txn)
    }
  end

  defp validate_range({:key, key}, key_type) do
    {:key, Exmdb.encode(key, key_type)}
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
  defp direction({:key, from}, {:key, to}) when from <= to, do: :fwd
  defp direction(_from, _to), do: :bwd
end

defimpl Enumerable, for: Exmdb.Range do
  def count(_range) do
    { :error, __MODULE__ }
  end

  def member?(%Exmdb.Range{db_spec: {dbi, key_type, val_type}}, {key, val}) do
    {:ok, case :elmdb.get(dbi, Exmdb.encode(key, key_type)) do
            {:ok, bin} ->
              Exmdb.decode(bin, val_type) == val
            :not_found ->
              false
            {:error, {_code, msg}} ->
              raise List.to_string(msg)
          end}
  end

  def reduce(%Exmdb.Range{from: from, to: to, direction: dir, db_spec: db_spec} = range, acc, fun) do
    {dbi, key_type, val_type} = db_spec
    with {:ok, txn, txn_to_close, txn_type} <- get_txn(range),
         {:ok, cur} <- cursor_open(txn, dbi, txn_type) do
      case start(cur, key_type, val_type, txn_type, from, to, dir, acc, fun) do
        {:cont, acc} ->
          close(cur, txn_to_close, txn_type)
          {:done, acc}
        {:suspend, acc} ->
          {:suspended, acc}
        {:halt, acc} ->
          close(cur, txn_to_close, txn_type)
          {:halted, acc}
        {:error, {_code, msg}} ->
          raise List.to_string(msg)
      end
    else
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end

  defp start(cur, key_type, val_type, txn_type, from, to, dir, {:cont, acc}, fun) do
    with {init_op, cont_op, limit} <- prepare(cur, from, to, dir),
         {:ok, key, val} <- cursor_get(cur, init_op, txn_type) do
      key = Exmdb.decode(key, key_type)
      acc = fun.({key, Exmdb.decode(val, val_type)}, acc)
      reduce_cursor(cur, key_type, val_type, txn_type, cont_op, limit, acc, fun)
    else
      :not_found ->
        {:cont, acc}
      error ->
        error
    end
  end

  defp reduce_cursor(cur, key_type, val_type, txn_type, op, to, {:cont, acc}, fun) do
    case apply(cur, key_type, val_type, txn_type, op, to, acc, fun) do
      {:ok, acc} ->
        reduce_cursor(cur, key_type, val_type, txn_type, op, to, acc, fun)
      done ->
        done
    end
  end
  defp reduce_cursor(_cur, _key_type, _val_type, _txn_type, _op, _to, { :halt, acc }, _fun) do
    { :halt, acc }
  end
  defp reduce_cursor(cur, key_type, val_type, txn_type, op, to, { :suspend, acc }, fun) do
    { :suspend, acc, &reduce_cursor(cur, key_type, val_type, txn_type, op, to, &1, fun) }
  end

  defp apply(cur, key_type, val_type, txn_type, op, to, acc, fun) do
    case cursor_get(cur, op, txn_type) do
      {:ok, key, val} ->
        if binkey_in_range?(key, op, to) do
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

  defp prepare(cur, from, to, dir) do
    limit = get_limit(to)
    cont_op = get_cont_op(dir)
    case get_init_op(from, dir, cur) do
      {:error, e} ->
        {:error, e}
      init_op ->
        {init_op, cont_op, limit}
    end
  end

  defp get_limit({:key, key}), do: key
  defp get_limit(other), do: other

  defp get_cont_op(:fwd), do: :next
  defp get_cont_op(:bwd), do: :prev

  defp get_init_op(:first, _dir, _cur), do: :first
  defp get_init_op(:last, _dir, _cur), do: :last
  defp get_init_op({:key, key}, :fwd, _cur), do: {:set_range, key}
  defp get_init_op({:key, key}, :bwd, cur) do
    case :elmdb.ro_txn_cursor_get(cur, {:set_range, key}) do
      {:ok, ^key, _val} ->
        {:set, key}
      {:ok, _key, _val} ->
        case :elmdb.ro_txn_cursor_get(cur, :prev) do
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

  defp get_txn(%Exmdb.Range{txn: nil, env_res: env_res}) do
    case :elmdb.ro_txn_begin(env_res) do
      {:ok, txn} ->
        {:ok, txn, txn, :ro}
      error ->
        error
    end
  end
  defp get_txn(%Exmdb.Range{txn: txn}) do
    {:ok, txn.res, nil, txn.type}
  end

  defp cursor_open(txn, dbi, :ro) do
    :elmdb.ro_txn_cursor_open(txn, dbi)
  end
  defp cursor_open(txn, dbi, :rw) do
    :elmdb.txn_cursor_open(txn, dbi)
  end

  defp cursor_get(txn, op, :ro) do
    :elmdb.ro_txn_cursor_get(txn, op)
  end
  defp cursor_get(txn, op, :rw) do
    :elmdb.txn_cursor_get(txn, op)
  end

  defp binkey_in_range?(binkey, :next, to) do
    to == :last or binkey <= to
  end
  defp binkey_in_range?(binkey, :prev, to) do
    to == :first or binkey >= to
  end

  defp close(cur, nil, :ro) do
    :elmdb.ro_txn_cursor_close(cur)
  end
  defp close(_cur, nil, :rw) do
    :ok
  end
  defp close(cur, txn, :ro) do
    :elmdb.ro_txn_abort(txn)
    :elmdb.ro_txn_cursor_close(cur)
  end
  defp close(_cur, txn, :rw) do
    :elmdb.txn_commit(txn)
  end
end
