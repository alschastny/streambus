<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

enum DeleteMode: string
{
    case KeepRef = 'KEEPREF'; // default — keep PEL reference on delete
    case DelRef  = 'DELREF';  // remove PEL reference on delete
    case Acked   = 'ACKED';   // only delete if all groups have acked
}
