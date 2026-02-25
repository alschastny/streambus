<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

enum IdmpMode
{
    case None;     // no idempotency (default)
    case Auto;     // IDMPAUTO — Redis deduplicates by content hash
    case Explicit; // IDMP — caller supplies idempotentId per message
}
