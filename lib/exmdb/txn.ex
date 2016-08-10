defmodule Exmdb.Txn do
  defstruct res: nil, env: nil, type: nil

  @type t :: %Exmdb.Txn{res: binary, env: Exmdb.Env.t, type: :rw | :ro}
end
