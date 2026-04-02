box.cfg {
    listen = 3301
}

box.once('init', function()
    local kv = box.schema.space.create('KV', {
        format = {
            {name = 'key', type = 'string'},
            {name = 'value', type = 'varbinary', is_nullable = true},
        },
        if_not_exists = true,
    })

    kv:create_index('primary', {
        parts = {'key'},
        type = 'TREE',
        if_not_exists = true,
    })

    box.schema.user.create('kvuser', {
        password = 'kvpassword',
        if_not_exists = true,
    })

    box.schema.user.grant('kvuser', 'read,write', 'space', 'KV', {
        if_not_exists = true,
    })

    box.schema.user.grant('kvuser', 'execute', 'universe', nil, {
        if_not_exists = true,
    })
end)

print("Tarantool KV ready")