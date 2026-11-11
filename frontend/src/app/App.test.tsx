import { screen } from '@testing-library/react';
import { renderApp } from '../test/render';
import { App } from './App';
import { NAV } from './Layout';

describe('App', () => {
  it('shows a not-found page with a way back for unknown routes', () => {
    renderApp(<App />, { route: '/nowhere' });

    expect(screen.getByRole('heading', { name: 'Page not found' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Go to the overview' })).toHaveAttribute('href', '/');
    expect(screen.getByRole('link', { name: 'Skip to content' })).toHaveAttribute('href', '#main');
  });

  it('links every dashboard section from the primary navigation', () => {
    renderApp(<App />, { route: '/nowhere' });
    const nav = screen.getByRole('navigation', { name: 'Primary' });
    for (const item of NAV) {
      expect(nav).toContainElement(screen.getByRole('link', { name: item.label }));
    }
    expect(NAV.map((n) => n.label)).toEqual([
      'Overview',
      'Live stream',
      'Kafka & processing',
      'Analytics',
      'Dead letters',
      'Simulations',
      'System',
    ]);
  });

  it('shows the operator state as read-only until unlocked', () => {
    renderApp(<App />, { route: '/nowhere' });
    expect(screen.getByRole('button', { name: 'Read-only · Unlock' })).toBeInTheDocument();
  });
});
