<?php

namespace Nortuni\Estoria\Ouro;

use GuzzleHttp\Client;
use Nortuni\Estoria\EventStore;
use Nortuni\Estoria\Stream;
use Nortuni\Estoria\StreamId;

final class OuroEventStore implements EventStore
{
    /**
     * @var string
     */
    private $uri;

    /**
     * @var Client
     */
    private $httpClient;

    public function __construct($uri)
    {
        $this->uri = $uri;
        $this->httpClient = new Client();
    }

    public function getStream(StreamId $streamId)
    {
        return new OuroStream($this->httpClient, $this->uri, $streamId, true);
    }

    /**
     * @param StreamId $streamId
     * @return Stream
     */
    public function getOrCreateStream(StreamId $streamId)
    {
        return new OuroStream($this->httpClient, $this->uri, $streamId);
    }
}