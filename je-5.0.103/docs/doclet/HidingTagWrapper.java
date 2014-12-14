/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.Doc;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;

class HidingTagWrapper extends HidingWrapper implements Tag {
    public HidingTagWrapper(Tag tag, Map mapWrappers) {
        super(tag, mapWrappers);
    }

    private Tag _getTag() {
        return (Tag)getWrappedObject();
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getTag().firstSentenceTags());
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getTag().inlineTags());
    }

    public String kind() {
        return _getTag().kind();
    }

    public String name() {
        return _getTag().name();
    }

    public String text() {
        return _getTag().text();
    }

    public String toString() {
        return _getTag().toString();
    }
  
    public SourcePosition position() {
        return (SourcePosition) wrapOrHide(_getTag().position());
    }
  
    public Doc holder() {
        return (Doc) wrapOrHide(_getTag().holder());
    }
}
