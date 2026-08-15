import { useState, type FormEvent } from 'react';
import { useAuthStore } from '../../stores/authStore';
import { Button, TextField } from '../ui';

export function LoginPage() {
  const [password, setPassword] = useState('');
  const { login, loading, error } = useAuthStore();

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    login(password);
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-bg">
      <form
        onSubmit={handleSubmit}
        className="bg-surface-raised p-8 rounded-lg border border-border-subtle w-80"
      >
        <h1 className="text-xl font-semibold mb-6 text-center">Nerve</h1>
        <TextField
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          aria-label="Password"
          autoFocus
          className="mb-4"
        />
        {error && <p className="text-error text-sm mb-3">{error}</p>}
        {/* `type="submit"` is explicit: Button defaults to `button`, because
            almost none of the app's buttons submit a form. This one does. */}
        <Button type="submit" variant="primary" size="md" fullWidth disabled={loading}>
          {loading ? '...' : 'Login'}
        </Button>
      </form>
    </div>
  );
}
