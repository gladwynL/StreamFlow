import { screen } from '@testing-library/react';
import { jsonResponse, liveSnapshot } from '../../test/fixtures';
import { feedOf, renderApp } from '../../test/render';
import { KafkaPage } from './KafkaPage';

describe('KafkaPage', () => {
  it('shows each partition stored against its retention limit and flags a lagging partition near it', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(jsonResponse({ processing: null, deadLetterIngestion: null }))),
    );
    renderApp(<KafkaPage />, { feed: feedOf(liveSnapshot()) });

    expect(
      screen.getByRole('columnheader', { name: 'Stored / retention limit' }),
    ).toBeInTheDocument();
    expect(screen.getByText('52.5 MiB · 5.1 %')).toBeInTheDocument();
    expect(screen.getByText('870.7 MiB · 85.0 % · at risk')).toBeInTheDocument();
  });
});
