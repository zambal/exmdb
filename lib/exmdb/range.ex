defmodule Exmdb.Range do
  defstruct from: :"$exmdb_first", to: :"$exmdb_last", env: nil, db: nil

end

defimpl Enumerable, for: Exmdb.Range do
  def count(_range) do
    { :error, __MODULE__ }
  end

  def member?(%Exmdb.Range{env: env, db: db}, {key, val}) do
    {:ok, case Exmdb.get(env, key, :"$exmdb_no_member", db: db) do
            :"$exmdb_no_member" -> false
            ^val -> true
            _ -> false
          end}
  end

  def reduce(%Exmdb.Range{from: from, to: to, env: env, db: db}, acc, fun) do
    {dbi, key_type, val_type} = Exmdb.expand_db_spec(env.dbs, db: db)
    with {:ok, txn} <- :elmdb.ro_txn_begin(env.res),
         {:ok, cur} <- :elmdb.ro_txn_cursor_open(txn, dbi) do
      case start(cur, key_type, val_type, from, to, acc, fun) do
        {:cont, acc} ->
          close(txn, cur)
          {:done, acc}
        {:suspend, acc} ->
          {:suspended, acc}
        {:halt, acc} ->
          close(txn, cur)
          {:halted, acc}
        {:error, {_code, msg}} ->
          close(txn, cur)
          raise List.to_string(msg)
      end
    else
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end

  defp start(cur, key_type, val_type, from, to, {:cont, acc}, fun) do
    dir = direction(from, to)
    with {op, from, to} <- init(cur, from, to, dir, key_type),
         {:ok, key, val} <- :elmdb.ro_txn_cursor_get(cur, from) do
      key = Exmdb.decode(key, key_type)
      acc = fun.({key, Exmdb.decode(val, val_type)}, acc)
      reduce_cursor(cur, key_type, val_type, op, to, acc, fun)
    else
      :not_found ->
        {:cont, acc}
      error ->
        error
    end
  end

  defp reduce_cursor(cur, key_type, val_type, op, to, {:cont, acc}, fun) do
    case apply(cur, key_type, val_type, op, to, acc, fun) do
      {:ok, acc} ->
        reduce_cursor(cur, key_type, val_type, op, to, acc, fun)
      done ->
        done
    end
  end
  defp reduce_cursor(_cur, _key_type, _val_type, _op, _to, { :halt, acc }, _fun) do
    { :halt, acc }
  end
  defp reduce_cursor(cur, key_type, val_type, op, to, { :suspend, acc }, fun) do
    { :suspend, acc, &reduce_cursor(cur, key_type, val_type, op, to, &1, fun) }
  end

  defp apply(cur, key_type, val_type, op, to, acc, fun) do
    case :elmdb.ro_txn_cursor_get(cur, op) do
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

  defp init(cur, from, to, dir, type) do
    op = if dir == :fwd, do: :next, else: :prev
    from = to_op(from, type)
    to = to_op(to, type)
    cond do
      from in [:first, :last] ->
        {op, from, to}
      dir == :fwd ->
        {op, {:set_range, from}, to}
      dir == :bwd ->
        case find_at_most(cur, from) do
          {:set, key} ->
            {op, {:set, key}, to}
          :last ->
            {op, :last, to}
          done ->
            done
        end
    end
  end

  defp find_at_most(cur, from) do
    case :elmdb.ro_txn_cursor_get(cur, {:set_range, from}) do
      {:ok, key, _val} when key == from ->
        {:set, key}
      {:ok, key, _val} ->
        case :elmdb.ro_txn_cursor_get(cur, :prev) do
          {:ok, key, _val} ->
            {:set, key}
          other ->
            other
        end
      :not_found ->
        :last
      error ->
        error
    end
  end

  defp to_op(:"$exmdb_first", _type), do: :first
  defp to_op(:"$exmdb_last", _type), do: :last
  defp to_op(key, type), do: Exmdb.encode(key, type)

  defp binkey_in_range?(binkey, :next, to) do
    to == :last or binkey <= to
  end
  defp binkey_in_range?(binkey, :prev, to) do
    to == :first or binkey >= to
  end

  defp direction(:"$exmdb_first", _to), do: :fwd
  defp direction(:"$exmdb_last", _to), do: :bwd
  defp direction(_from, :"$exmdb_last"), do: :fwd
  defp direction(_from, :"$exmdb_first"), do: :bwd
  defp direction(from, to) when from <= to, do: :fwd
  defp direction(_from, _to), do: :bwd

  defp close(txn, cur) do
    :elmdb.ro_txn_abort(txn)
    :elmdb.ro_txn_cursor_close(cur)
  end
end
