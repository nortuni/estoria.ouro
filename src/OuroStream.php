<?php

namespace Taphper\GetEventStore;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Message\ResponseInterface;
use Nortuni\Estoria\Commit;
use Nortuni\Estoria\PersistedEvent;
use Nortuni\Estoria\Stream;
use Nortuni\Estoria\StreamId;
use Nortuni\Estoria\StreamNotFound;

final class OuroStream implements Stream
{
    /**
     * @var Client
     */
    private $httpClient;

    /**
     * @var string
     */
    private $uri;

    /**
     * @var mixed
     */
    private $json;

    /**
     * @var int
     */
    private $key = 0;

    /**
     * @var int
     */
    private $readEntriesOnPage = 0;

    public function __construct(Client $httpClient, $uri, StreamId $streamId, $checkExistence = false)
    {
        $this->httpClient = $httpClient;
        $this->uri = $uri;
        $this->streamId = $streamId;
        if($checkExistence) {
            try {
                $this->initialise();
            } catch(ClientException $exception) {
                throw new StreamNotFound();
            }
        }
    }

    public function current()
    {
        $entries = count($this->json['entries']);

        $entry = $this->json['entries'][$entries - 1 - $this->readEntriesOnPage];

        foreach($entry['links'] as $link) {
            if($link['relation'] == 'alternate') {
                $event = $this->getEvent($link['uri']);
                return new PersistedEvent(
                    $this->streamId,
                    $event->eventNumber,
                    $event->eventType,
                    (object) $event->data,
                    (object) ($event->metadata ?: [])
                );
            }
        }

        return null; //exception?
    }

    public function next()
    {
        $this->readEntriesOnPage++;
        $this->key++;
    }

    public function key()
    {
        return $this->key;
    }

    public function valid()
    {
        if(!$this->hasPageUnreadEntries() && $this->hasLink('previous')) {
            $this->json = $this->getJson($this->getLink('previous'));
            $this->readEntriesOnPage = 0;
        }

        return $this->hasPageUnreadEntries();
    }

    public function rewind()
    {
        if(!is_array($this->json)) {
            $this->initialise();
        }

        if($this->hasLink('last')) {
            $this->json = $this->getJson($this->getLink('last'));
        }

        $this->readEntriesOnPage = 0;
        $this->key = 0;
    }

    public function append(Commit $commit)
    {
        $serializedEvents = json_encode($commit->getEvents());

        $request = $this
            ->httpClient
            ->createRequest(
                'POST',
                $this->uri . '/streams/' . (string) $this->streamId,
                [
                    'body' => $serializedEvents
                ]
            )
            ->setHeader('Content-Type','application/vnd.eventstore.events+json')
        ;

        $response = $this->sendHelper($request);

        $responseStatusCode = $response->getStatusCode();

        if ('201' != $responseStatusCode) {
            throw new \Exception('Could not write commit');
        }
    }

    /**
     * @param $uri
     * @return mixed
     */
    private function getJson($uri)
    {
        $request = $this->httpClient->createRequest(
                'GET',
                $uri,
                [
                    'headers' => [
                        'Accept' => 'application/json'
                    ]
                ]
            );

        /** @var ResponseInterface $response */
        $response = $this->sendHelper($request);
        return $response->json();
    }

    /**
     * @param $relation
     * @return string
     */
    private function getLink($relation)
    {
        foreach($this->json['links'] as $link) {
            if($link['relation'] == $relation) {
                return $link['uri'];
            }
        }

        throw new \InvalidArgumentException("No relation $relation found.");
    }

    /**
     * @param $relation
     * @return bool
     */
    private function hasLink($relation)
    {
        if(!is_array($this->json) || !array_key_exists('links', $this->json)) {
            return false;
        }

        foreach($this->json['links'] as $link) {
            if($link['relation'] == $relation) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return bool
     */
    private function hasPageUnreadEntries()
    {
        return count($this->json['entries']) > $this->readEntriesOnPage;
    }

    private function getEvent($uri)
    {
        $request = $this->httpClient->createRequest(
            'GET',
            $uri,
            [
                'headers' => [
                    'Accept' => 'application/vnd.eventstore.atom+json'
                ]
            ]
        );

        /** @var ResponseInterface $response */
        $response = $this->sendHelper($request);
        $json = $response->json();
        return (object) $json['content'];
    }

    private function initialise()
    {
        $this->json = $this->getJson($this->uri . '/streams/' . (string) $this->streamId);
    }

    /**
     * @param $request
     * @return ResponseInterface
     */
    private function sendHelper($request)
    {
        return $this->httpClient->send($request);
    }
}
