import { useState } from "react";
import { supportApi } from "../api/client";
import { Send, CheckCircle, AlertCircle, Loader2, LifeBuoy } from "lucide-react";

export default function SupportPanel({ user }) {
  const [name,    setName]    = useState(user?.username ?? "");
  const [subject, setSubject] = useState("");
  const [message, setMessage] = useState("");
  const [status,  setStatus]  = useState("idle"); // idle | sending | success | error
  const [errMsg,  setErrMsg]  = useState("");

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!name.trim() || !subject.trim() || !message.trim()) return;
    setStatus("sending");
    setErrMsg("");
    try {
      await supportApi.contact({ name: name.trim(), subject: subject.trim(), message: message.trim() });
      setStatus("success");
      setSubject("");
      setMessage("");
    } catch (err) {
      const msg = err?.response?.data?.error ?? "Failed to send message. Please try again.";
      setErrMsg(msg);
      setStatus("error");
    }
  };

  return (
    <div className="flex-1 overflow-y-auto p-6 bg-slate-950 min-h-0">
      <div className="max-w-2xl mx-auto">

        {/* Header */}
        <div className="flex items-center gap-3 mb-8">
          <div className="p-2.5 rounded-xl bg-brand-600/20 border border-brand-600/30">
            <LifeBuoy className="w-5 h-5 text-brand-400" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-slate-100">Support</h1>
            <p className="text-sm text-slate-400">Send a message to our team and we'll get back to you.</p>
          </div>
        </div>

        {/* Success state */}
        {status === "success" && (
          <div className="flex items-start gap-3 bg-emerald-900/20 border border-emerald-700/40 rounded-xl p-4 mb-6">
            <CheckCircle className="w-5 h-5 text-emerald-400 shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-semibold text-emerald-300">Message sent</p>
              <p className="text-sm text-emerald-400/80 mt-0.5">We've received your message and will respond as soon as possible.</p>
            </div>
          </div>
        )}

        {/* Error state */}
        {status === "error" && (
          <div className="flex items-start gap-3 bg-red-900/20 border border-red-700/40 rounded-xl p-4 mb-6">
            <AlertCircle className="w-5 h-5 text-red-400 shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-semibold text-red-300">Failed to send</p>
              <p className="text-sm text-red-400/80 mt-0.5">{errMsg}</p>
            </div>
          </div>
        )}

        {/* Form */}
        <form onSubmit={handleSubmit} className="bg-slate-900 border border-slate-800 rounded-2xl p-6 flex flex-col gap-5">

          <div className="flex flex-col gap-1.5">
            <label className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Your Name</label>
            <input
              type="text"
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="Enter your name"
              maxLength={100}
              required
              className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2.5 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/40 transition"
            />
          </div>

          <div className="flex flex-col gap-1.5">
            <label className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Email</label>
            <input
              type="text"
              value={user?.email ?? ""}
              readOnly
              className="bg-slate-800/50 border border-slate-700/50 rounded-lg px-3 py-2.5 text-sm text-slate-400 cursor-not-allowed"
            />
          </div>

          <div className="flex flex-col gap-1.5">
            <label className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Subject</label>
            <input
              type="text"
              value={subject}
              onChange={e => setSubject(e.target.value)}
              placeholder="What's this about?"
              maxLength={200}
              required
              className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2.5 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/40 transition"
            />
          </div>

          <div className="flex flex-col gap-1.5">
            <label className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
              Message
              <span className="ml-2 text-slate-600 normal-case font-normal">{message.length}/5000</span>
            </label>
            <textarea
              value={message}
              onChange={e => setMessage(e.target.value)}
              placeholder="Describe your issue or question in detail..."
              maxLength={5000}
              required
              rows={7}
              className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2.5 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/40 transition resize-y"
            />
          </div>

          <button
            type="submit"
            disabled={status === "sending" || !name.trim() || !subject.trim() || !message.trim()}
            className="flex items-center justify-center gap-2 bg-brand-600 hover:bg-brand-500 disabled:bg-slate-700 disabled:text-slate-500 disabled:cursor-not-allowed text-white font-semibold text-sm px-5 py-2.5 rounded-lg transition"
          >
            {status === "sending"
              ? <><Loader2 className="w-4 h-4 animate-spin" /> Sending…</>
              : <><Send className="w-4 h-4" /> Send Message</>
            }
          </button>

        </form>
      </div>
    </div>
  );
}
